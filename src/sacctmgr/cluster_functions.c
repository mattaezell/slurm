/*****************************************************************************\
 *  cluster_functions.c - functions dealing with clusters in the
 *                        accounting system.
 *****************************************************************************
 *  Copyright (C) 2008 Lawrence Livermore National Security.
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *  LLNL-CODE-402394.
 *  
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.llnl.gov/linux/slurm/>.
 *  
 *  SLURM is free software; you can redistribute it and/or modify it under
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
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "src/sacctmgr/sacctmgr.h"

static int _print_file_sacctmgr_assoc_list(
	FILE *fd, List sacctmgr_assoc_list, List user_list, List acct_list);

static int _set_cond(int *start, int argc, char *argv[],
		     List cluster_list,
		     List format_list)
{
	int i;
	int set = 0;
	int end = 0;

	for (i=(*start); i<argc; i++) {
		end = parse_option_end(argv[i]);
		if (strncasecmp (argv[i], "Set", 3) == 0) {
			i--;
			break;
		} else if(!end && !strncasecmp(argv[i], "where", 5)) {
			continue;
		} else if(!end) {
			addto_char_list(cluster_list, argv[i]);
			set = 1;
		} else if (strncasecmp (argv[i], "Format", 1) == 0) {
			if(format_list)
				addto_char_list(format_list, argv[i]+end);
		} else if (strncasecmp (argv[i], "Names", 1) == 0) {
			addto_char_list(cluster_list,
					argv[i]+end);
			set = 1;
		} else {
			printf(" Unknown condition: %s\n"
			       "Use keyword set to modify value\n", argv[i]);
		}
	}
	(*start) = i;

	return set;
}

static int _set_rec(int *start, int argc, char *argv[],
		    acct_association_rec_t *assoc)
{
	int i, mins;
	int set = 0;
	int end = 0;

	for (i=(*start); i<argc; i++) {
		end = parse_option_end(argv[i]);
		if (strncasecmp (argv[i], "Where", 5) == 0) {
			i--;
			break;
		} else if(!end && !strncasecmp(argv[i], "set", 3)) {
			continue;
		} else if(!end) {
			printf(" Bad format on %s: End your option with "
			       "an '=' sign\n", argv[i]);			
		} else if (strncasecmp (argv[i], "FairShare", 1) == 0) {
			if (get_uint(argv[i]+end, &assoc->fairshare, 
			    "FairShare") == SLURM_SUCCESS)
				set = 1;
		} else if (strncasecmp (argv[i], "MaxJobs", 4) == 0) {
			if (get_uint(argv[i]+end, &assoc->max_jobs,
			    "MaxJobs") == SLURM_SUCCESS)
				set = 1;
		} else if (strncasecmp (argv[i], "MaxNodes", 4) == 0) {
			if (get_uint(argv[i]+end, 
			    &assoc->max_nodes_per_job,
			    "MaxNodes") == SLURM_SUCCESS)
				set = 1;
		} else if (strncasecmp (argv[i], "MaxWall", 4) == 0) {
			mins = time_str2mins(argv[i]+end);
			if (mins != NO_VAL) {
				assoc->max_wall_duration_per_job
						= (uint32_t) mins;
				set = 1;
			} else {
				printf(" Bad MaxWall time format: %s\n", 
					argv[i]);
			}
		} else if (strncasecmp (argv[i], "MaxCPUSecs", 4) == 0) {
			if (get_uint(argv[i]+end, 
			     &assoc->max_cpu_secs_per_job, 
			    "MaxCPUSecs") == SLURM_SUCCESS)
				set = 1;
		} else {
			printf(" Unknown option: %s\n"
			       " Use keyword 'where' to modify condition\n",
			       argv[i]);
		}
	}
	(*start) = i;

	return set;

}

static int _print_file_sacctmgr_assoc_childern(FILE *fd, 
					       List sacctmgr_assoc_list,
					       List user_list,
					       List acct_list)
{
	ListIterator itr = NULL;
	sacctmgr_assoc_t *sacctmgr_assoc = NULL;
	char *line = NULL;
	acct_user_rec_t *user_rec = NULL;
	acct_account_rec_t *acct_rec = NULL;

	itr = list_iterator_create(sacctmgr_assoc_list);
	while((sacctmgr_assoc = list_next(itr))) {
		if(sacctmgr_assoc->assoc->user) {
			user_rec = sacctmgr_find_user_from_list(
				user_list, sacctmgr_assoc->assoc->user);
			line = xstrdup_printf(
				"User - %s", sacctmgr_assoc->sort_name);
			if(user_rec) {
				xstrfmtcat(line, ":DefaultAccount=%s",
					   user_rec->default_acct);
				if(user_rec->admin_level > ACCT_ADMIN_NONE)
					xstrfmtcat(line, ":AdminLevel=%s",
						   acct_admin_level_str(
							   user_rec->
							   admin_level));
				if(user_rec->qos > ACCT_QOS_NORMAL)
					xstrfmtcat(line, ":QOS=%s",
						   acct_qos_str(user_rec->qos));
			}
		} else {
			acct_rec = sacctmgr_find_account_from_list(
				acct_list, sacctmgr_assoc->assoc->acct);
			line = xstrdup_printf(
				"Account - %s", sacctmgr_assoc->sort_name);
			if(acct_rec) {
				xstrfmtcat(line, ":Description='%s'",
					   acct_rec->description);
				xstrfmtcat(line, ":Organization='%s'",
					   acct_rec->organization);
				if(acct_rec->qos > ACCT_QOS_NORMAL)
					xstrfmtcat(line, ":QOS=%s",
						   acct_qos_str(acct_rec->qos));
			}
		}
		if(sacctmgr_assoc->assoc->fairshare != INFINITE)
			xstrfmtcat(line, ":Fairshare=%u", 
				   sacctmgr_assoc->assoc->fairshare);
		
		if(sacctmgr_assoc->assoc->max_cpu_secs_per_job != INFINITE)
			xstrfmtcat(line, ":MaxCPUSecs=%u",
				   sacctmgr_assoc->assoc->max_cpu_secs_per_job);
		
		if(sacctmgr_assoc->assoc->max_jobs != INFINITE) 
			xstrfmtcat(line, ":MaxJobs=%u",
				   sacctmgr_assoc->assoc->max_jobs);
		
		if(sacctmgr_assoc->assoc->max_nodes_per_job != INFINITE)
			xstrfmtcat(line, ":MaxNodes=%u",
				   sacctmgr_assoc->assoc->max_nodes_per_job);
		
		if(sacctmgr_assoc->assoc->max_wall_duration_per_job 
		   != INFINITE)
 			xstrfmtcat(line, ":MaxWallDurationPerJob=%u",
				   sacctmgr_assoc->assoc->
				   max_wall_duration_per_job);


		if(fprintf(fd, "%s\n", line) < 0) {
			error("Can't write to file");
			return SLURM_ERROR;
		}
		info("%s", line);
	}
	list_iterator_destroy(itr);
	_print_file_sacctmgr_assoc_list(fd, sacctmgr_assoc_list,
					user_list, acct_list);

	return SLURM_SUCCESS;
}

static int _print_file_sacctmgr_assoc_list(FILE *fd, 
					   List sacctmgr_assoc_list,
					   List user_list,
					   List acct_list)
{
	ListIterator itr = NULL;
	sacctmgr_assoc_t *sacctmgr_assoc = NULL;

	itr = list_iterator_create(sacctmgr_assoc_list);
	while((sacctmgr_assoc = list_next(itr))) {
/* 		info("got here %d with %d from %s %s",  */
/* 		     depth, list_count(sacctmgr_assoc->childern), */
/* 		     sacctmgr_assoc->assoc->acct, sacctmgr_assoc->assoc->user); */
		if(!list_count(sacctmgr_assoc->childern))
			continue;
		if(fprintf(fd, "Parent - %s\n",
			   sacctmgr_assoc->assoc->acct) < 0) {
			error("Can't write to file");
			return SLURM_ERROR;
		}
		info("%s - %s", "Parent",
		       sacctmgr_assoc->assoc->acct);
/* 		info("sending %d from %s", */
/* 		     list_count(sacctmgr_assoc->childern), */
/* 		     sacctmgr_assoc->assoc->acct); */
		_print_file_sacctmgr_assoc_childern(
			fd, sacctmgr_assoc->childern, user_list, acct_list);
	}	
	list_iterator_destroy(itr);

	return SLURM_SUCCESS;
}

extern int sacctmgr_add_cluster(int argc, char *argv[])
{
	int rc = SLURM_SUCCESS;
	int i = 0, mins;
	acct_cluster_rec_t *cluster = NULL;
	List name_list = list_create(slurm_destroy_char);
	List cluster_list = NULL;
	uint32_t fairshare = NO_VAL; 
	uint32_t max_cpu_secs_per_job = NO_VAL;
	uint32_t max_jobs = NO_VAL;
	uint32_t max_nodes_per_job = NO_VAL;
	uint32_t max_wall_duration_per_job = NO_VAL;
	int limit_set = 0;
	ListIterator itr = NULL, itr_c = NULL;
	char *name = NULL;

	for (i=0; i<argc; i++) {
		int end = parse_option_end(argv[i]);
		if(!end) {
			addto_char_list(name_list, argv[i]+end);
		} else if (strncasecmp (argv[i], "FairShare", 1) == 0) {
			fairshare = atoi(argv[i]+end);
			limit_set = 1;
		} else if (strncasecmp (argv[i], "MaxCPUSecs", 4) == 0) {
			max_cpu_secs_per_job = atoi(argv[i]+end);
			limit_set = 1;
		} else if (strncasecmp (argv[i], "MaxJobs=", 4) == 0) {
			max_jobs = atoi(argv[i]+end);
			limit_set = 1;
		} else if (strncasecmp (argv[i], "MaxNodes", 4) == 0) {
			max_nodes_per_job = atoi(argv[i]+end);
			limit_set = 1;
		} else if (strncasecmp (argv[i], "MaxWall", 4) == 0) {
			mins = time_str2mins(argv[i]+end);
			if (mins != NO_VAL) {
				max_wall_duration_per_job = (uint32_t) mins;
				limit_set = 1;
			} else {
				printf(" Bad MaxWall time format: %s\n", 
					argv[i]);
			}
		} else if (strncasecmp (argv[i], "Names", 1) == 0) {
			addto_char_list(name_list, argv[i]+end);
		} else {
			printf(" Unknown option: %s\n", argv[i]);
		}		
	}

	if(!list_count(name_list)) {
		list_destroy(name_list);
		printf(" Need name of cluster to add.\n"); 
		return SLURM_ERROR;
	} else {
		List temp_list = NULL;
		acct_cluster_cond_t cluster_cond;
		char *name = NULL;

		memset(&cluster_cond, 0, sizeof(acct_cluster_cond_t));
		cluster_cond.cluster_list = name_list;

		temp_list = acct_storage_g_get_clusters(db_conn, &cluster_cond);
		if(!temp_list) {
			printf(" Problem getting clusters from database.  "
		   	    "Contact your admin.\n");
			return SLURM_ERROR;
		}

		itr_c = list_iterator_create(name_list);
		itr = list_iterator_create(temp_list);
		while((name = list_next(itr_c))) {
			acct_cluster_rec_t *cluster_rec = NULL;

			list_iterator_reset(itr);
			while((cluster_rec = list_next(itr))) {
				if(!strcasecmp(cluster_rec->name, name))
					break;
			}
			if(cluster_rec) {
				printf(" This cluster %s already exists.  "
				       "Not adding.\n", name);
				list_delete_item(itr_c);
			}
		}
		list_iterator_destroy(itr);
		list_iterator_destroy(itr_c);
		list_destroy(temp_list);
		if(!list_count(name_list)) {
			list_destroy(name_list);
			return SLURM_ERROR;
		}
	}

	printf(" Adding Cluster(s)\n");
	cluster_list = list_create(destroy_acct_cluster_rec);
	itr = list_iterator_create(name_list);
	while((name = list_next(itr))) {
		cluster = xmalloc(sizeof(acct_cluster_rec_t));
		cluster->name = xstrdup(name);
		list_append(cluster_list, cluster);

		printf("  Name          = %s\n", cluster->name);

		cluster->default_fairshare = fairshare;		
		cluster->default_max_cpu_secs_per_job = max_cpu_secs_per_job;
		cluster->default_max_jobs = max_jobs;
		cluster->default_max_nodes_per_job = max_nodes_per_job;
		cluster->default_max_wall_duration_per_job = 
			max_wall_duration_per_job;
	}
	list_iterator_destroy(itr);
	list_destroy(name_list);

	if(limit_set) {
		printf(" User Defaults\n");
		if(fairshare == INFINITE)
			printf("  Fairshare       = NONE\n");
		else if(fairshare != NO_VAL) 
			printf("  Fairshare       = %u\n", fairshare);
		
		if(max_cpu_secs_per_job == INFINITE)
			printf("  MaxCPUSecs      = NONE\n");
		else if(max_cpu_secs_per_job != NO_VAL) 
			printf("  MaxCPUSecs      = %u\n",
			       max_cpu_secs_per_job);
		
		if(max_jobs == INFINITE) 
			printf("  MaxJobs         = NONE\n");
		else if(max_jobs != NO_VAL) 
			printf("  MaxJobs         = %u\n", max_jobs);
		
		if(max_nodes_per_job == INFINITE)
			printf("  MaxNodes        = NONE\n");
		else if(max_nodes_per_job != NO_VAL)
			printf("  MaxNodes        = %u\n", max_nodes_per_job);
		
		if(max_wall_duration_per_job == INFINITE) 
			printf("  MaxWall         = NONE\n");		
		else if(max_wall_duration_per_job != NO_VAL) {
			char time_buf[32];
			mins2time_str((time_t) max_wall_duration_per_job, 
				      time_buf, sizeof(time_buf));
			printf("  MaxWall         = %s\n", time_buf);
		}
	}

	if(!list_count(cluster_list)) {
		printf(" Nothing new added.\n");
		goto end_it;
	}

	notice_thread_init();
	rc = acct_storage_g_add_clusters(db_conn, my_uid, cluster_list);
	notice_thread_fini();
	if(rc == SLURM_SUCCESS) {
		if(commit_check("Would you like to commit changes?")) {
			acct_storage_g_commit(db_conn, 1);
		} else {
			printf(" Changes Discarded\n");
			acct_storage_g_commit(db_conn, 0);
		}
	} else {
		printf(" error: problem adding clusters\n");
	}
end_it:
	list_destroy(cluster_list);
	
	return rc;
}

extern int sacctmgr_list_cluster(int argc, char *argv[])
{
	int rc = SLURM_SUCCESS;
	acct_cluster_cond_t *cluster_cond =
		xmalloc(sizeof(acct_cluster_cond_t));
	List cluster_list;
	int i=0;
	ListIterator itr = NULL;
	ListIterator itr2 = NULL;
	acct_cluster_rec_t *cluster = NULL;
	char *object;

	print_field_t *field = NULL;

	List format_list = list_create(slurm_destroy_char);
	List print_fields_list; /* types are of print_field_t */

	enum {
		PRINT_CLUSTER,
		PRINT_CHOST,
		PRINT_CPORT,
		PRINT_FAIRSHARE,
		PRINT_MAXC,
		PRINT_MAXJ,
		PRINT_MAXN,
		PRINT_MAXW
	};


	cluster_cond->cluster_list = list_create(slurm_destroy_char);
	_set_cond(&i, argc, argv, cluster_cond->cluster_list, format_list);
	
	cluster_list = acct_storage_g_get_clusters(db_conn, cluster_cond);
	destroy_acct_cluster_cond(cluster_cond);
	
	if(!cluster_list) {
		printf(" Problem with query.\n");
		list_destroy(format_list);
		return SLURM_ERROR;
	}

	print_fields_list = list_create(destroy_print_field);

	if(!list_count(format_list)) {
		addto_char_list(format_list, 
				"Cl,Controlh,Controlp,F,MaxC,MaxJ,MaxN,MaxW");
	}

	itr = list_iterator_create(format_list);
	while((object = list_next(itr))) {
		field = xmalloc(sizeof(print_field_t));
		if(!strncasecmp("Cluster", object, 2)) {
			field->type = PRINT_CLUSTER;
			field->name = xstrdup("Cluster");
			field->len = 10;
			field->print_routine = print_fields_str;
		} else if(!strncasecmp("ControlHost", object, 8)) {
			field->type = PRINT_CHOST;
			field->name = xstrdup("Control Host");
			field->len = 12;
			field->print_routine = print_fields_str;
		} else if(!strncasecmp("ControlPort", object, 8)) {
			field->type = PRINT_CPORT;
			field->name = xstrdup("Control Port");
			field->len = 12;
			field->print_routine = print_fields_uint;
		} else if(!strncasecmp("FairShare", object, 1)) {
			field->type = PRINT_FAIRSHARE;
			field->name = xstrdup("FairShare");
			field->len = 9;
			field->print_routine = print_fields_uint;
		} else if(!strncasecmp("MaxCPUSecs", object, 4)) {
			field->type = PRINT_MAXC;
			field->name = xstrdup("MaxCPUSecs");
			field->len = 11;
			field->print_routine = print_fields_uint;
		} else if(!strncasecmp("MaxJobs", object, 4)) {
			field->type = PRINT_MAXJ;
			field->name = xstrdup("MaxJobs");
			field->len = 7;
			field->print_routine = print_fields_uint;
		} else if(!strncasecmp("MaxNodes", object, 4)) {
			field->type = PRINT_MAXN;
			field->name = xstrdup("MaxNodes");
			field->len = 8;
			field->print_routine = print_fields_uint;
		} else if(!strncasecmp("MaxWall", object, 4)) {
			field->type = PRINT_MAXW;
			field->name = xstrdup("MaxWall");
			field->len = 11;
			field->print_routine = print_fields_time;
		} else {
			printf("Unknown field '%s'\n", object);
			xfree(field);
			continue;
		}
		list_append(print_fields_list, field);		
	}
	list_iterator_destroy(itr);
	list_destroy(format_list);

	itr = list_iterator_create(cluster_list);
	itr2 = list_iterator_create(print_fields_list);
	print_fields_header(print_fields_list);

	while((cluster = list_next(itr))) {
		while((field = list_next(itr2))) {
			switch(field->type) {
			case PRINT_CLUSTER:
				field->print_routine(SLURM_PRINT_VALUE, field,
						     cluster->name);
				break;
			case PRINT_CHOST:
				field->print_routine(SLURM_PRINT_VALUE, field,
						     cluster->control_host);
				break;
			case PRINT_CPORT:
				field->print_routine(SLURM_PRINT_VALUE, field,
						     cluster->control_port);
				break;
			case PRINT_FAIRSHARE:
				field->print_routine(
					SLURM_PRINT_VALUE, field,
					cluster->default_fairshare);
				break;
			case PRINT_MAXC:
				field->print_routine(
					SLURM_PRINT_VALUE, field,
					cluster->default_max_cpu_secs_per_job);
				break;
			case PRINT_MAXJ:
				field->print_routine(
					SLURM_PRINT_VALUE, field, 
					cluster->default_max_jobs);
				break;
			case PRINT_MAXN:
				field->print_routine(
					SLURM_PRINT_VALUE, field,
					cluster->default_max_nodes_per_job);
				break;
			case PRINT_MAXW:
				field->print_routine(
					SLURM_PRINT_VALUE, field,
					cluster->
					default_max_wall_duration_per_job);
				break;
			default:
				break;
			}
		}
		list_iterator_reset(itr2);
		printf("\n");
	}

	list_iterator_destroy(itr2);
	list_iterator_destroy(itr);
	list_destroy(cluster_list);
	list_destroy(print_fields_list);

	return rc;
}

extern int sacctmgr_modify_cluster(int argc, char *argv[])
{
	int rc = SLURM_SUCCESS;
	int i=0;
	acct_association_rec_t *assoc = xmalloc(sizeof(acct_association_rec_t));
	acct_association_cond_t *assoc_cond =
		xmalloc(sizeof(acct_association_cond_t));
	int cond_set = 0, rec_set = 0, set = 0;
	List ret_list = NULL;

	assoc_cond = xmalloc(sizeof(acct_association_cond_t));
	assoc_cond->cluster_list = list_create(slurm_destroy_char);
	assoc_cond->acct_list = list_create(NULL);
	assoc_cond->fairshare = NO_VAL;
	assoc_cond->max_cpu_secs_per_job = NO_VAL;
	assoc_cond->max_jobs = NO_VAL;
	assoc_cond->max_nodes_per_job = NO_VAL;
	assoc_cond->max_wall_duration_per_job = NO_VAL;
	
	assoc->fairshare = NO_VAL;
	assoc->max_cpu_secs_per_job = NO_VAL;
	assoc->max_jobs = NO_VAL;
	assoc->max_nodes_per_job = NO_VAL;
	assoc->max_wall_duration_per_job = NO_VAL;

	for (i=0; i<argc; i++) {
		if (strncasecmp (argv[i], "Where", 5) == 0) {
			i++;
			if(_set_cond(&i, argc, argv,
				     assoc_cond->cluster_list, NULL))
				cond_set = 1;
		} else if (strncasecmp (argv[i], "Set", 3) == 0) {
			i++;
			if(_set_rec(&i, argc, argv, assoc))
				rec_set = 1;
		} else {
			if(_set_cond(&i, argc, argv,
				     assoc_cond->cluster_list, NULL))
				cond_set = 1;
		}
	}

	if(!rec_set) {
		printf(" You didn't give me anything to set\n");
		destroy_acct_association_rec(assoc);
		destroy_acct_association_cond(assoc_cond);
		return SLURM_ERROR;
	} else if(!cond_set) {
		if(!commit_check("You didn't set any conditions with 'WHERE'.\n"
				 "Are you sure you want to continue?")) {
			printf("Aborted\n");
			destroy_acct_association_rec(assoc);
			destroy_acct_association_cond(assoc);
			return SLURM_SUCCESS;
		}		
	}

	printf(" Setting\n");
	if(rec_set) 
		printf(" User Defaults  =\n");

	if(assoc->fairshare == INFINITE)
		printf("  Fairshare     = NONE\n");
	else if(assoc->fairshare != NO_VAL) 
		printf("  Fairshare     = %u\n", assoc->fairshare);
		
	if(assoc->max_cpu_secs_per_job == INFINITE)
		printf("  MaxCPUSecs    = NONE\n");
	else if(assoc->max_cpu_secs_per_job != NO_VAL) 
		printf("  MaxCPUSecs    = %u\n",
		       assoc->max_cpu_secs_per_job);
		
	if(assoc->max_jobs == INFINITE) 
		printf("  MaxJobs       = NONE\n");
	else if(assoc->max_jobs != NO_VAL) 
		printf("  MaxJobs       = %u\n", assoc->max_jobs);
		
	if(assoc->max_nodes_per_job == INFINITE)
		printf("  MaxNodes      = NONE\n");
	else if(assoc->max_nodes_per_job != NO_VAL)
		printf("  MaxNodes      = %u\n",
		       assoc->max_nodes_per_job);
		
	if(assoc->max_wall_duration_per_job == INFINITE) 
		printf("  MaxWall       = NONE\n");		
	else if(assoc->max_wall_duration_per_job != NO_VAL) {
		char time_buf[32];
		mins2time_str((time_t) 
			      assoc->max_wall_duration_per_job, 
			      time_buf, sizeof(time_buf));
		printf("  MaxWall       = %s\n", time_buf);
	}

	list_append(assoc_cond->acct_list, "root");
	notice_thread_init();
	ret_list = acct_storage_g_modify_associations(
		db_conn, my_uid, assoc_cond, assoc);
	
	if(ret_list && list_count(ret_list)) {
		char *object = NULL;
		ListIterator itr = list_iterator_create(ret_list);
		printf(" Modified cluster defaults for associations...\n");
		while((object = list_next(itr))) {
			printf("  %s\n", object);
		}
		list_iterator_destroy(itr);
		set = 1;
	} else if(ret_list) {
		printf(" Nothing modified\n");
	} else {
		printf(" Error with request\n");
		rc = SLURM_ERROR;
	}

	if(ret_list)
		list_destroy(ret_list);
	notice_thread_fini();

	if(set) {
		if(commit_check("Would you like to commit changes?")) 
			acct_storage_g_commit(db_conn, 1);
		else {
			printf(" Changes Discarded\n");
			acct_storage_g_commit(db_conn, 0);
		}
	}
	destroy_acct_association_cond(assoc_cond);
	destroy_acct_association_rec(assoc);

	return rc;
}

extern int sacctmgr_delete_cluster(int argc, char *argv[])
{
	int rc = SLURM_SUCCESS;
	acct_cluster_cond_t *cluster_cond =
		xmalloc(sizeof(acct_cluster_cond_t));
	int i=0;
	List ret_list = NULL;

	cluster_cond->cluster_list = list_create(slurm_destroy_char);
	
	if(!_set_cond(&i, argc, argv, cluster_cond->cluster_list, NULL)) {
		printf(" No conditions given to remove, not executing.\n");
		destroy_acct_cluster_cond(cluster_cond);
		return SLURM_ERROR;
	}

	if(!list_count(cluster_cond->cluster_list)) {
		destroy_acct_cluster_cond(cluster_cond);
		return SLURM_SUCCESS;
	}
	notice_thread_init();
	ret_list = acct_storage_g_remove_clusters(
		db_conn, my_uid, cluster_cond);
	notice_thread_fini();

	destroy_acct_cluster_cond(cluster_cond);

	if(ret_list && list_count(ret_list)) {
		char *object = NULL;
		ListIterator itr = list_iterator_create(ret_list);
		printf(" Deleting clusters...\n");
		while((object = list_next(itr))) {
			printf("  %s\n", object);
		}
		list_iterator_destroy(itr);
		if(commit_check("Would you like to commit changes?")) {
			acct_storage_g_commit(db_conn, 1);
		} else {
			printf(" Changes Discarded\n");
			acct_storage_g_commit(db_conn, 0);
		}
	} else if(ret_list) {
		printf(" Nothing deleted\n");
	} else {
		printf(" Error with request\n");
		rc = SLURM_ERROR;
	}

	if(ret_list)
		list_destroy(ret_list);

	return rc;
}

extern int sacctmgr_dump_cluster (int argc, char *argv[])
{
	acct_association_cond_t assoc_cond;
	List assoc_list = NULL;
	List acct_list = NULL;
	List user_list = NULL;
	List sacctmgr_assoc_list = NULL;
	char *cluster_name = NULL;
	char *file_name = NULL;
	int i;
	FILE *fd = NULL;

	for (i=0; i<argc; i++) {
		int end = parse_option_end(argv[i]);
		if(!end) {
			if(cluster_name) {
				printf(" Can only do one cluster at a time.  "
				       "Already doing %s\n", cluster_name);
				continue;
			}
			cluster_name = xstrdup(argv[i]+end);
		} else if (strncasecmp (argv[i], "File", 1) == 0) {
			if(file_name) {
				printf(" File name already set to %s\n",
				       file_name);
				continue;
			}		
			file_name = xstrdup(argv[i]+end);
		} else if (strncasecmp (argv[i], "Name", 1) == 0) {
			if(cluster_name) {
				printf(" Can only do one cluster at a time.  "
				       "Already doing %s\n", cluster_name);
				continue;
			}
			cluster_name = xstrdup(argv[i]+end);
		} else {
			printf(" Unknown option: %s\n", argv[i]);
		}		
	}

	if(!cluster_name) {
		printf(" We need a cluster to dump.\n");
		return SLURM_ERROR;
	}

	if(!file_name) {
		file_name = xstrdup_printf("./%s.cfg", cluster_name);
		printf(" No filename give using %s.\n", file_name);
	}

	memset(&assoc_cond, 0, sizeof(acct_association_cond_t));

	assoc_cond.without_parent_limits = 1;
	assoc_cond.cluster_list = list_create(NULL);
	list_append(assoc_cond.cluster_list, cluster_name);

	user_list = acct_storage_g_get_users(db_conn, NULL);
	acct_list = acct_storage_g_get_accounts(db_conn, NULL);
	assoc_list = acct_storage_g_get_associations(db_conn, &assoc_cond);
	list_destroy(assoc_cond.cluster_list);
	if(!assoc_list) {
		printf(" Problem with query.\n");
		xfree(cluster_name);
		return SLURM_ERROR;
	} else if(!list_count(assoc_list)) {
		printf(" Cluster %s returned nothing.", cluster_name);
		xfree(cluster_name);
		return SLURM_ERROR;
	}

	sacctmgr_assoc_list = sacctmgr_get_hierarchical_list(assoc_list);
	
	fd = fopen(file_name, "w");
	/* Add header */
	if(fprintf(fd,
		   "# To edit this file start with a cluster line "
		   "for the new cluster\n"
		   "# Cluster - cluster_name\n"
		   "# Followed by Accounts you want in this fashion...\n"
		   "# Account - cs:MaxNodesPerJob=5:MaxJobs=4:"
		   "MaxProcSecondsPerJob=20:FairShare=399:"
		   "MaxWallDurationPerJob=40:Description='Computer Science':"
		   "Organization='LC'\n"
		   "# Any of the options after a ':' can be left out and "
		   "they can be in any order.\n"
		   "# If you want to add any sub accounts just list the "
		   "Parent THAT HAS ALREADY \n"
		   "# BEEN CREATED before the account line in this fashion...\n"
		   "# Parent - cs\n"
		   "# Account - test:MaxNodesPerJob=1:MaxJobs=1:"
		   "MaxProcSecondsPerJob=1:FairShare=1:MaxWallDurationPerJob=1:"
		   "Description='Test Account':Organization='Test'\n"
		   "# To add users to a account add a line like this after a "
		   "Parent - line\n"
		   "# User - lipari:MaxNodesPerJob=2:MaxJobs=3:"
		   "MaxProcSecondsPerJob=4:FairShare=1:"
		   "MaxWallDurationPerJob=1\n") < 0) {
		error("Can't write to file");
		return SLURM_ERROR;
	}

	if(fprintf(fd, "Cluster - %s\n", cluster_name) < 0) {
		error("Can't write to file");
		return SLURM_ERROR;
	}

	_print_file_sacctmgr_assoc_list(
		fd, sacctmgr_assoc_list, user_list, acct_list);

	xfree(cluster_name);
	xfree(file_name);
	list_destroy(sacctmgr_assoc_list);
	list_destroy(assoc_list);
	fclose(fd);

	return SLURM_SUCCESS;
}
