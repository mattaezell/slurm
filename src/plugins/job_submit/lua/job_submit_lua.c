/*****************************************************************************\
 *  job_submit_lua.c - Set defaults in job submit request specifications.
 *****************************************************************************
 *  Copyright (C) 2010 Lawrence Livermore National Security.
 *  Portions Copyright (C) 2010-2017 SchedMD LLC <https://www.schedmd.com>.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include <dlfcn.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/slurm_xlator.h"
#include "src/common/assoc_mgr.h"
#include "src/common/xlua.h"
#include "src/common/slurm_lua.h"
#include "src/common/uid.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/reservation.h"

#define _DEBUG 0
#define MIN_ACCTG_FREQUENCY 30

/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting the type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *	<application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "auth" for Slurm authentication) and <method> is a
 * description of how this plugin satisfies that application.  Slurm will
 * only load authentication plugins if the plugin_type string has a prefix
 * of "auth/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */
const char plugin_name[]       	= "Job submit lua plugin";
const char plugin_type[]       	= "job_submit/lua";
const uint32_t plugin_version   = SLURM_VERSION_NUMBER;

static const char lua_script_path[] = DEFAULT_SCRIPT_DIR "/job_submit.lua";
static time_t lua_script_last_loaded = (time_t) 0;
static lua_State *L = NULL;

const char *fns[] = {
	"slurm_job_submit",
	"slurm_job_modify",
	NULL
};   
time_t last_lua_jobs_update = (time_t) 0;
time_t last_lua_resv_update = (time_t) 0;

/*
 *  Mutex for protecting multi-threaded access to this plugin.
 *   (Only 1 thread at a time should be in here)
 */
static pthread_mutex_t lua_lock = PTHREAD_MUTEX_INITIALIZER;

/* These are defined here so when we link with something other than
 * the slurmctld we will have these symbols defined.  They will get
 * overwritten when linking with the slurmctld.
 */
#if defined (__APPLE__)
int accounting_enforce __attribute__((weak_import)) = 0;
void *acct_db_conn  __attribute__((weak_import)) = NULL;
#else
int accounting_enforce = 0;
void *acct_db_conn = NULL;
#endif

/*****************************************************************************\
 * We've provided a simple example of the type of things you can do with this
 * plugin. If you develop another plugin that may be of interest to others
 * please post it to slurm-dev@schedmd.com  Thanks!
\*****************************************************************************/

/*
 *  NOTE: The init callback should never be called multiple times,
 *   let alone called from multiple threads. Therefore, locking
 *   is unnecessary here.
 */
int init(void)
{
	int rc = SLURM_SUCCESS;

	/* Read lock on jobs */
	slurmctld_lock_t job_read_lock =
		{ NO_LOCK, READ_LOCK, NO_LOCK, NO_LOCK, NO_LOCK };

	/*
	 * Need to dlopen() the Lua library to ensure plugins see
	 * appropriate symptoms
	 */
	if ((rc = xlua_dlopen()) != SLURM_SUCCESS)
		return rc;

	rc = slurm_lua_load_script(&L, lua_script_path, &lua_script_last_loaded, fns);

	lock_slurmctld(job_read_lock);
	slurm_lua_update_jobs_global(L, job_list, &last_lua_jobs_update);
	slurm_lua_update_resvs_global(L, resv_list, &last_lua_resv_update);
	unlock_slurmctld(job_read_lock);

	return rc;
}

int fini(void)
{
	lua_close (L);
	return SLURM_SUCCESS;
}


/* Lua script hook called for "submit job" event. */
extern int job_submit(struct job_descriptor *job_desc, uint32_t submit_uid,
		      char **err_msg)
{
	int rc = SLURM_ERROR;
	char *user_msg = NULL;
	slurm_mutex_lock (&lua_lock);

	(void) slurm_lua_load_script(&L, lua_script_path, &lua_script_last_loaded, fns);
	slurm_lua_update_jobs_global(L, job_list, &last_lua_jobs_update);
	slurm_lua_update_resvs_global(L, resv_list, &last_lua_resv_update);

	/*
	 *  All lua script functions should have been verified during
	 *   initialization:
	 */
	lua_getglobal(L, "slurm_job_submit");
	if (lua_isnil(L, -1))
		goto out;

	slurm_lua_update_jobs_global(L, job_list, &last_lua_jobs_update);
	slurm_lua_update_resvs_global(L, resv_list, &last_lua_resv_update);

	slurm_lua_push_job_desc(L, job_desc);
	slurm_lua_push_partition_list(L, part_list, job_desc->user_id, submit_uid);
	lua_pushnumber (L, submit_uid);
	slurm_lua_stack_dump("job_submit, before lua_pcall", L);
	if (lua_pcall (L, 3, 1, 0) != 0) {
		error("%s/lua: %s: %s",
		      __func__, lua_script_path, lua_tostring (L, -1));
	} else {
		if (lua_isnumber(L, -1)) {
			rc = lua_tonumber(L, -1);
		} else {
			info("%s/lua: %s: non-numeric return code",
			      __func__, lua_script_path);
			rc = SLURM_SUCCESS;
		}
		lua_pop(L, 1);
	}
	slurm_lua_stack_dump("job_submit, after lua_pcall", L);

	user_msg = slurm_lua_get_user_msg(L);
	if (user_msg) 
		*err_msg = user_msg;

out:	slurm_mutex_unlock (&lua_lock);
	return rc;
}

/* Lua script hook called for "modify job" event. */
extern int job_modify(struct job_descriptor *job_desc,
		      struct job_record *job_ptr, uint32_t submit_uid)
{
	int rc = SLURM_ERROR;
	char *user_msg = NULL;
	slurm_mutex_lock (&lua_lock);

	/*
	 *  All lua script functions should have been verified during
	 *   initialization:
	 */
	lua_getglobal(L, "slurm_job_modify");
	if (lua_isnil(L, -1))
		goto out;

	slurm_lua_update_jobs_global(L, job_list, &last_lua_jobs_update);
	slurm_lua_update_resvs_global(L, resv_list, &last_lua_resv_update);

	slurm_lua_push_job_desc(L, job_desc);
	slurm_lua_push_job_rec(L, job_ptr);
	slurm_lua_push_partition_list(L, part_list, job_ptr->user_id, submit_uid);
	lua_pushnumber (L, submit_uid);
	slurm_lua_stack_dump("job_modify, before lua_pcall", L);
	if (lua_pcall (L, 4, 1, 0) != 0) {
		error("%s/lua: %s: %s",
		      __func__, lua_script_path, lua_tostring (L, -1));
	} else {
		if (lua_isnumber(L, -1)) {
			rc = lua_tonumber(L, -1);
		} else {
			info("%s/lua: %s: non-numeric return code",
			     __func__, lua_script_path);
			rc = SLURM_SUCCESS;
		}
		lua_pop(L, 1);
	}
	slurm_lua_stack_dump("job_modify, after lua_pcall", L);

	user_msg = slurm_lua_get_user_msg(L);
	if (user_msg) {
		error("Use of log.user() in job_modify is not supported. "
		      "Message discarded: (\"%s\")", user_msg);
		xfree(user_msg);
	}

out:	slurm_mutex_unlock (&lua_lock);
	return rc;
}
