/*****************************************************************************\
 *  slurm_lua.h - Lua integration common functions
 *****************************************************************************
 *  Copyright (C) 2015 SchedMD LLC.
 *  Written by Tim Wickberg <tim@schedmd.com>
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

#ifndef _SLURM_LUA_H
#define _SLURM_LUA_H

#include <dlfcn.h>
#include <stdio.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"
#include "src/common/log.h"
#include "src/slurmctld/slurmctld.h"

#ifdef HAVE_LUA
# include <lua.h>
# include <lauxlib.h>
# include <lualib.h>
#else
# define LUA_VERSION_NUM 0
#endif

void slurm_lua_stack_dump(char *prefix, lua_State *L);
int slurm_lua_log_lua_msg(const char *prefix, lua_State *L);
char *slurm_lua_get_user_msg(lua_State *L);
int slurm_lua_load_script(lua_State **LI, const char *lua_script_path, time_t *lua_script_last_loaded, const char *fns[]);
void slurm_lua_push_job_desc(lua_State *L, struct job_descriptor *job_desc);
void slurm_lua_push_job_rec(lua_State *L, struct job_record *job_ptr);
void slurm_lua_push_partition_list(lua_State *L, List part_list, uint32_t user_id, uint32_t submit_uid);
void slurm_lua_update_jobs_global(lua_State *L, List job_list, time_t *last_lua_jobs_update);
void slurm_lua_update_resvs_global(lua_State *L, List resv_list, time_t *last_lua_resv_update);

#endif
