/************************************************************
	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <math.h>
#include <time.h>

# if defined(_MSC_VER)
# ifndef _CRT_SECURE_NO_DEPRECATE
# define _CRT_SECURE_NO_DEPRECATE (1)
# endif
# pragma warning(disable : 4996)
# endif

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);
		g_command = argv[1];
		/* Test code */
		/*
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
		*/
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);					
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    	/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
	      tmp_tok_ptr = tok_ptr->next;
	      free(tok_ptr);
	      tok_ptr=tmp_tok_ptr;
		}
	}
	printf("rc=%d\n", rc);

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
	    else if (*cur == '\'')
	    {
	      /* Find STRING_LITERRAL */
			int t_class;
	      	cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

	      	temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
	      	else /* must be a ' */
	      	{
	        	add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
	        	cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
	        	}
	      	}
	    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
				((cur->next != NULL) && (cur->next->tok_value == K_TABLE)) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DELETE) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_UPDATE) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_SELECT))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) && 
				((cur->next != NULL) && (cur->next->tok_value == K_TO)) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("BACKUP TO statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_RESTORE) &&
					((cur->next != NULL) && (cur->next->tok_value == K_FROM)) && (g_tpd_list->db_flags == ROLLFORWARD_DONE))
	{
		printf("RESTORE FROM statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_ROLLFORWARD)
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else
	{
		if (g_tpd_list->db_flags == ROLLFORWARD_PENDING)
		{
			printf("ROLLFORWARD_PENDING, unable to process until ROLLFORWARD is done\n");
			rc = INVALID_ROLLFORWARD_PENDING;
			cur->tok_value = INVALID;
		}
		else
		{ 
			printf("Invalid statement\n");
			rc = cur_cmd;
			cur->tok_value = INVALID;
		}
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			case INSERT:
						rc = sem_insert(cur);
						break;
			case DELETE:
						rc = sem_delete(cur);
						break;
			case UPDATE:
						rc = sem_update(cur);
						break;
			case SELECT:
						rc = sem_select(cur);
						break;
			case BACKUP:
						rc = sem_backup(cur);
						break;
			case RESTORE:
						rc = sem_restore(cur);
						break;
			case ROLLFORWARD:
						rc = sem_rollforward(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	if (!rc && !internal_command && 
		(cur_cmd == CREATE_TABLE || cur_cmd == DROP_TABLE || cur_cmd == INSERT || cur_cmd == DELETE || cur_cmd == UPDATE))
	{
		rc = write_to_log(g_command, cur_cmd);
	}

	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              				/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                				/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  	tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);
						free(new_entry);

						FILE *fhandle = NULL;
						table_file_header new_table;
						table_file_header *new_table_ptr = NULL;
						
						char filename[MAX_TOK_LEN] = "";
						strcpy(filename, tab_entry.table_name);
						strcat(filename, ".tab");

						if ((fhandle = fopen(filename, "wbc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
						else
						{
							new_table.file_size = sizeof(table_file_header);
							new_table.record_size = cur_id;												// Every coloumn has one byte to indicate the length
							for (int j = 0; j < cur_id; j++)
							{
								if (col_entry[j].col_type == T_INT)
									new_table.record_size += sizeof(int);
								else
									new_table.record_size += (sizeof(char) * col_entry[j].col_len);
							}
							if (new_table.record_size % BOUNDARY != 0)
								new_table.record_size = (((new_table.record_size) / BOUNDARY) + 1) * BOUNDARY;   // Rounding the record size to a multiple of the boundary
							new_table.num_records = 0;
							new_table.record_offset = sizeof(table_file_header);
							new_table.file_header_flag = 0;
							new_table.tpd_ptr = NULL;
							new_table_ptr = (table_file_header*)calloc(1, new_table.file_size);

							if (new_table_ptr == NULL)
							{
								rc = MEMORY_ERROR;
							}
							else
							{
								memcpy((void*)new_table_ptr, (void*)&new_table, new_table.file_size);
								fwrite(new_table_ptr, sizeof(table_file_header), 1, fhandle);

								free(new_table_ptr);
								fflush(fhandle);
								fclose(fhandle);
								printf("tablename is %s\n", filename);
							}
						}

					}
				}
			}
		}
	}

	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				if (remove(strcat(cur->tok_string, ".tab")) != 0)
					rc = DELETE_TABLE_ERROR;
			}
		}
	}

	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0, cur_id = 0, i = 0, str_length = 0, int_value = 0;
	char int_max_string[10];
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char *tuple, *tuple_cur;
	bool insert_done = false;
	token_list *cur;
	FILE *fhandle = NULL;
	table_file_header *table = NULL;

	cur = t_list;

	if (cur->next == NULL)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else if ((cur->next->tok_value != K_VALUES))
	{
		printf("INVALID_STATEMENT\nvalues keyword missing\n");
		rc = INVALID_STATEMENT;
		cur->next->tok_value = INVALID;
	}
	else
	{
		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			printf("INVALID_TABLE_NAME\n");
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) 		//check if table exsits
			{
				printf("TABLE_NOT_EXIST\n");
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next->next;									//skipping VALUES

				if (cur->tok_value != S_LEFT_PAREN)
				{
					//Error
					printf("INVALID_INSERT_SYNTAX\nmissing '('\n");
					rc = INVALID_INSERT_SYNTAX;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;

					char filename[MAX_TOK_LEN] = "";
					strcpy(filename, tab_entry->table_name);
					strcat(filename, ".tab");
					rc = read_table_from_file(filename, &table);

					if (!rc)
					{
						if (table->num_records == MAX_NUM_RECORDS)
						{
							printf("MAX_ROW_EXCEEDED\n");
							rc = MAX_ROW_EXCEEDED;
							cur->tok_value = INVALID;
						}
						else
						{
							tuple = (char*)calloc(1, table->record_size);
							if (tuple == NULL)
							{
								rc = MEMORY_ERROR;
							}
							else
							{
								tuple_cur = tuple;

								for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_entry++)
								{
									if (cur->tok_value == K_NULL)
									{
										if (col_entry->not_null)
										{
											printf("not null constraint\n");
											rc = INVALID_NULL_CONSTRAINT;
											cur->tok_value = INVALID;
											break;
										}
										else
										{
											memset((void*)tuple_cur++, '\0', 1);
											memset((void*)tuple_cur, '\0', col_entry->col_len);
											tuple_cur += col_entry->col_len;
											cur = cur->next;
										}
									}
									else if (col_entry->col_type == T_CHAR)
									{

										if (cur->tok_value != STRING_LITERAL)
										{
											printf("type mismatch\n");
											rc = TYPE_MISMATCH;
											cur->tok_value = INVALID;
											break;
										}
										else
										{
											str_length = strlen(cur->tok_string);
											if (col_entry->col_len < str_length)
											{
												rc = MAX_LENGTH_EXCEEDED;
												cur->tok_value = INVALID;
												break;
											}
											else
											{
												memcpy((void*)tuple_cur++, (void*)&str_length, 1);
												memcpy((void*)tuple_cur, (void*)&cur->tok_string, col_entry->col_len);
												tuple_cur += col_entry->col_len;
												cur = cur->next;
											}
										}

									}
									else     //must be T_INT
									{
										if (cur->tok_value != INT_LITERAL)
										{
											printf("type mismatch\n");
											rc = TYPE_MISMATCH;
											cur->tok_value = INVALID;
											break;
										}
										else
										{
											itoa(INT_MAX, int_max_string, 10);
											if ((strlen(cur->tok_string) > strlen(int_max_string)) ||
												((strlen(cur->tok_string) > strlen(int_max_string)) && (strcmp(cur->tok_string, int_max_string) > 0)))
											{
												rc = MAX_INT_EXCEEDED;
												cur->tok_value = INVALID;
												break;
											}
											else
											{
												int_value = atoi(cur->tok_string);
												memcpy((void*)tuple_cur++, (void*)&col_entry->col_len, 1);
												memcpy((void*)tuple_cur, (void*)&int_value, col_entry->col_len);
												tuple_cur += col_entry->col_len;
												cur = cur->next;
											}
										}
									}

									if (i != tab_entry->num_columns - 1)
									{
										if (cur->tok_value != S_COMMA)												//Not the last column and next tok is not comma
										{
											printf("number of columns and insert values don't match\n");
											rc = INVALID_INSERT_SYNTAX;
											cur->tok_value = INVALID;
											break;
										}
										else
										{
											cur = cur->next;
										}
									}
								}

								if (!rc)
								{
									if ((cur->tok_value != S_RIGHT_PAREN) || (cur->next->tok_value != EOC))
									{
										rc = INVALID_INSERT_SYNTAX;
										cur->tok_value = INVALID;
									}
									else
									{
										if ((fhandle = fopen(filename, "wbc")) == NULL)
										{
											rc = FILE_OPEN_ERROR;
										}
										else
										{
											int old_file_size = table->file_size;

											table->file_size += table->record_size;
											table->num_records++;
											fwrite(table, old_file_size, 1, fhandle);
											fwrite(tuple, table->record_size, 1, fhandle);
											fflush(fhandle);
											fclose(fhandle);

											printf("%s new size = %d\n", filename, table->file_size);
										}
									}
								}
								free(tuple);
							}
						}
						free(table);
					}
				}
			}
		}
	}

	return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0, i, j, k, num_col = 0, func = 0;
	token_list *cur, *select_cur;
	char tab_name[MAX_IDENT_LEN + 1];
	tpd_entry *tab_entry = NULL, *tab_entry2 = NULL;
	table_file_header *table = NULL, *table2 = NULL;
	FILE *fhandle = NULL;
	char *table_cur = NULL, *record_cur = NULL, *records[MAX_NUM_RECORDS * MAX_NUM_RECORDS] = { NULL };
	cd_entry *col_entry = NULL, *col_cur = NULL, *group_by_col = NULL, *col_array[MAX_NUM_COL * 2] = { NULL };
	bool done = false, join = false;

	select_cur = t_list;
	cur = t_list;

	while (cur->tok_value != K_FROM && cur->tok_value != EOC)
	{
		cur = cur->next;
	}

	if (cur->tok_value == EOC)
	{
		printf("Missing keyword FROM\n");
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}

	if (!rc)			
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_name, cur->tok_string);
			if ((tab_entry = get_tpd_from_list(tab_name)) == NULL) 		//check if table exsits
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;

				char filename[MAX_TOK_LEN] = "";
				strcpy(filename, tab_entry->table_name);
				strcat(filename, ".tab");
				rc = read_table_from_file(filename, &table);
				if (!rc)
				{
					if (cur->tok_value == S_COMMA)			//checking for second table
					{
						cur = cur->next;

						if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
						{
							rc = INVALID_TABLE_NAME;
							cur->tok_value = INVALID;
						}
						else
						{
							strcpy(tab_name, cur->tok_string);
							if ((tab_entry2 = get_tpd_from_list(tab_name)) == NULL) 		//check if table exsits
							{
								rc = TABLE_NOT_EXIST;
								cur->tok_value = INVALID;
							}
							else
							{
								cur = cur->next;

								memset(filename, '\0', MAX_TOK_LEN);
								strcpy(filename, tab_entry2->table_name);
								strcat(filename, ".tab");
								rc = read_table_from_file(filename, &table2);

								if (!rc)
								{
									rc = join_table(&tab_entry, tab_entry2, &table, table2);

									if (!rc)
									{
										join = true;
										free(table2);
									}
								}
							}
						}
					}

					table_cur = ((char*)table + table->record_offset);

					if (!rc && (cur->tok_value == EOC || cur->tok_value == K_WHERE || cur->tok_value == K_ORDER || cur->tok_value == K_GROUP))
					{
						if (cur->tok_value == K_WHERE)
						{
							rc = where_clause_handler(&cur, tab_entry, table, records);
						}
						else // No where clause
						{
							for (i = 0; i < table->num_records; i++)
							{
								records[i] = table_cur;
								table_cur += table->record_size;
							}
						}

						if (cur->tok_value == K_GROUP && cur->next->tok_value == K_BY)
						{
							cur = cur->next->next;
							rc = get_cd_from_tpd(&cur, tab_entry, &group_by_col);

							if (!rc)
							{
								cur = cur->next;

								if (cur->tok_value != EOC)
								{
									rc = INVALID_STATEMENT;
									cur->tok_value = INVALID;
								}
							}
						}

						if (!rc)
						{
							if (!group_by_col && select_cur->tok_value == S_STAR && select_cur->next->tok_value == K_FROM)
							{
								for (i = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < tab_entry->num_columns; i++, col_cur++)
								{
									col_array[i] = col_cur;
								}
								num_col = i;
							}
							else
							{
								if (group_by_col)
								{
									if (strcmp(select_cur->tok_string, group_by_col->col_name) != 0)
									{
										printf("Column must be the same as the group by clause\n");
										rc = INVALID_GROUP_BY;
										select_cur->tok_value = INVALID;
										cur = select_cur;
									}
									else
									{
										select_cur = select_cur->next;

										if (select_cur->tok_value != S_COMMA)
										{
											rc = INVALID_STATEMENT;
											select_cur->tok_value = INVALID;
											cur = select_cur;
										}
										else
											select_cur = select_cur->next;
									}
								}

								if (!rc)
								{
									if (select_cur->tok_value == F_SUM || select_cur->tok_value == F_AVG || select_cur->tok_value == F_COUNT)
									{
										func = select_cur->tok_value;
										select_cur = select_cur->next;

										if (select_cur->tok_value != S_LEFT_PAREN || select_cur->next->next->tok_value != S_RIGHT_PAREN)
										{
											printf("Missing parenthese\n");
											rc = INVALID_SELECT_SYNTAX;
											select_cur->tok_value = INVALID;
											cur = select_cur;
										}
										else
										{
											select_cur = select_cur->next;				// points to the coloumn name
											if (func == F_COUNT && select_cur->tok_value == S_STAR)
											{
												rc = func_count(col_entry, tab_entry, table, records, group_by_col);
											}
											else
											{
												rc = get_cd_from_tpd(&select_cur, tab_entry, &col_entry);

												if (rc)  // error, match cur to select_cur
												{
													cur = select_cur;
												}
												else
												{
													if (func == F_SUM)
													{
														rc = func_sum_avg(col_entry, tab_entry, table, records, F_SUM, group_by_col);
													}
													else if (func == F_AVG)
													{
														rc = func_sum_avg(col_entry, tab_entry, table, records, F_AVG, group_by_col);
													}
													else // F_COUNT
													{
														rc = func_count(col_entry, tab_entry, table, records, group_by_col);
													}
												}
											}

										}

										if (cur->tok_value != EOC)
										{
											if (cur->tok_value == K_ORDER && cur->next->tok_value == K_BY)
											{
												printf("Sorry, Program does not support order by with SQL functions\n");
											}
										}

										if (!rc)		// move the select_cur passing col name and ')'
										{
											select_cur = select_cur->next->next;
										}
									}
									else if (!group_by_col)
									{
										i = 0;
										do
										{
											rc = get_cd_from_tpd(&select_cur, tab_entry, &col_entry);

											if (rc)  // error, match cur to select_cur
											{
												cur = select_cur;
											}
											else
											{
												col_array[i] = col_entry;
												select_cur = select_cur->next;

												if (select_cur->tok_value != S_COMMA)
												{
													done = true;
												}
												else
													select_cur = select_cur->next;
											}

											i++;
										} while (!rc && !done);

										num_col = i;
									}
								}
								

								if (!rc && select_cur->tok_value != K_FROM)
								{
									if (group_by_col)
										printf("GROUP BY must have a aggregate function in the select clause\n");
									rc = INVALID_SELECT_SYNTAX;
									select_cur->tok_value = INVALID;
									cur = select_cur;
								}
							}
						}

						if (!rc)
						{
							if (cur->tok_value == K_ORDER && cur->next->tok_value == K_BY)
							{
								cur = cur->next;
								rc = order_by_handler(&cur, tab_entry, table, records);
							}
							
						}

						if (!rc)
						{
							if (cur->tok_value != EOC)
							{
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
						}
					}
					else
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
					}

					if (!rc && !func)
					{						
						print_table(col_array, tab_entry, table->num_records, records, num_col);
					}

					free(table);

					if (join)
						free(tab_entry);
				}
			}
		}
	}

	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0, num_rows_affected = 0, i;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	token_list *cur;
	FILE *fhandle = NULL;
	table_file_header *table = NULL;

	cur = t_list;

	if (cur->tok_value != K_FROM)
	{
		printf("Missing keyword from\n");
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			printf("INVALID_TABLE_NAME\n");
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) 		//check if table exsits
			{
				printf("TABLE_NOT_EXIST error\n");
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;

				if (cur->tok_value != EOC && cur->tok_value != K_WHERE)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
				else
				{
					char filename[MAX_TOK_LEN] = "";
					strcpy(filename, tab_entry->table_name);
					strcat(filename, ".tab");
					rc = read_table_from_file(filename, &table);

					if (!rc)
					{
						if (table->num_records == 0)
						{
							printf("%s table is empty\n", tab_entry->table_name);
						}
						else
						{
							if (cur->tok_value == EOC)								//Delete all records
							{
								if ((fhandle = fopen(filename, "wbc")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
								}
								else
								{
									num_rows_affected = table->num_records;
									table->file_size = sizeof(table_file_header);
									table->num_records = 0;

									fwrite(table, table->file_size, 1, fhandle);
									fflush(fhandle);
									fclose(fhandle);
								}
							}
							else													//Delete with condition
							{
								char *records[MAX_NUM_RECORDS] = { NULL };
								char *last_record_cur = ((char*)table + table->record_offset + ((table->num_records - 1) * table->record_size));
								rc = where_clause_handler(&cur, tab_entry, table, records);

								if (!rc)
								{
									if (cur->tok_value != EOC)
									{
										rc = INVALID_STATEMENT;
										cur->tok_value = INVALID;
									}
									else
									{
										i = 0;
										while (i < table->num_records)
										{
											if (records[i] != NULL)
											{
												while (table->num_records > 0 && records[table->num_records - 1] != NULL) // Skipping last record if mark for delete
												{
													table->num_records--;
													last_record_cur -= table->record_size;
													num_rows_affected++;
													table->file_size -= table->record_size;
												}
												if (table->num_records && table->num_records != i)
												{
													memcpy(records[i], last_record_cur, table->record_size);
													records[i] = NULL;
													last_record_cur -= table->record_size;
													table->num_records--;
													num_rows_affected++;
													table->file_size -= table->record_size;
												}
											}
											i++;
										}

										if ((fhandle = fopen(filename, "wbc")) == NULL)
										{
											rc = FILE_OPEN_ERROR;
										}
										else
										{
											fwrite(table, table->file_size, 1, fhandle);
											fflush(fhandle);
											fclose(fhandle);
										}
									}
								}
							}
						}

						if (!rc)
						{
							printf("%d rows affected\n", num_rows_affected);
							if (num_rows_affected)
								printf("%s new size = %d\n", filename, table->file_size);
						}
						free(table);
					}																	
				}
			}
		}
	}

	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0, num_rows_affected = 0, update_value_int = 0, str_length = 0, i;
	char update_value[MAX_TOK_LEN + 1], int_max_string[10];
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	token_list *cur;
	FILE *fhandle = NULL;
	table_file_header *table = NULL;

	cur = t_list;

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		printf("INVALID_TABLE_NAME\n");
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL) 		//check if table exsits
		{
			printf("TABLE_NOT_EXIST error\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;

			if (cur->tok_value != K_SET)
			{
				printf("Missing keyword set\n");
				rc = INVALID_TABLE_NAME;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;

				rc = get_cd_from_tpd(&cur, tab_entry, &col_entry);

				if (!rc)
				{
					cur = cur->next;

					if (cur->tok_value != S_EQUAL)
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
					}
					else
					{
						cur = cur->next;
						memset(&update_value, '\0', sizeof(update_value));

						if (cur->tok_value == K_NULL)
						{
							if (col_entry->not_null)
							{
								printf("not null constraint\n");
								rc = INVALID_NULL_CONSTRAINT;
								cur->tok_value = INVALID;
							}
						}
						else if (col_entry->col_type == T_CHAR)
						{

							if (cur->tok_value != STRING_LITERAL)
							{
								printf("type mismatch\n");
								rc = TYPE_MISMATCH;
								cur->tok_value = INVALID;
							}
							else
							{
								str_length = strlen(cur->tok_string);

								if (col_entry->col_len < str_length)
								{
									rc = MAX_LENGTH_EXCEEDED;
									cur->tok_value = INVALID;
								}
								else
								{
									memcpy(&update_value, (void*)&str_length, 1);
									memcpy(&update_value[1], (void*)&cur->tok_string, col_entry->col_len);
								}
							}

						}
						else     //must be T_INT
						{
							if (cur->tok_value != INT_LITERAL)
							{
								printf("type mismatch\n");
								rc = TYPE_MISMATCH;
								cur->tok_value = INVALID;
							}
							else
							{
								itoa(INT_MAX, int_max_string, 10);
								if ((strlen(cur->tok_string) > strlen(int_max_string)) ||
									((strlen(cur->tok_string) > strlen(int_max_string)) && (strcmp(cur->tok_string, int_max_string) > 0)))
								{
									rc = MAX_INT_EXCEEDED;
									cur->tok_value = INVALID;
								}
								else
								{
									update_value_int = atoi(cur->tok_string);
									memcpy(&update_value, (void*)&col_entry->col_len, 1);
									memcpy(&update_value[1], (void*)&update_value_int, col_entry->col_len);
								}
							}
						}

						if (!rc)
						{
							cur = cur->next;

							if (cur->tok_value != EOC && cur->tok_value != K_WHERE)
							{
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
							else
							{
								char filename[MAX_TOK_LEN] = "";
								strcpy(filename, tab_entry->table_name);
								strcat(filename, ".tab");
								rc = read_table_from_file(filename, &table);
								if (!rc)
								{
									if (table->num_records == 0)
									{
										printf("%s table is empty\n", tab_entry->table_name);
									}
									else
									{
										int update_offset = 0, update_col_id = col_entry->col_id;
										char *update_cur = NULL;

										for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											i < update_col_id; i++, col_entry++)
										{
											update_offset++;
											update_offset += col_entry->col_len;
										}

										if (cur->tok_value == EOC)					//Update all records
										{
											for (i = 0, update_cur = (char*)table + table->record_offset + update_offset;
												i < table->num_records; i++)
											{
												memcpy(update_cur, (void*)&update_value, col_entry->col_len + 1);
												update_cur += table->record_size;
											}
											num_rows_affected = table->num_records;
										}
										else										//Update with condition
										{
											char *records[MAX_NUM_RECORDS] = { NULL };
											rc = where_clause_handler(&cur, tab_entry, table, records);

											if (!rc)
											{
												if (cur->tok_value != EOC)
												{
													rc = INVALID_STATEMENT;
													cur->tok_value = INVALID;
												}
												else
												{
													for (i = 0; i < table->num_records; i++)
													{
														if (records[i] != NULL)
														{
															update_cur = records[i] + update_offset;
															memcpy(update_cur, (void*)&update_value, col_entry->col_len + 1);
															num_rows_affected++;
														}
													}
												}
											}
										}

										if (!rc)
										{
											if ((fhandle = fopen(filename, "wbc")) == NULL)
											{
												rc = FILE_OPEN_ERROR;
											}
											else
											{
												fwrite(table, table->file_size, 1, fhandle);
												fflush(fhandle);
												fclose(fhandle);
											}
										}
									}

									if (!rc)
									{
										printf("%d rows affected\n", num_rows_affected);
									}
									free(table);
								}
							}
						}
					}
				}			
			}
		}
	}

	return rc;
}

int sem_backup(token_list *t_list)
{
	int rc = 0, i;
	char image_file_name[MAX_TOK_LEN + 1] = "";
	FILE *fhandle = NULL;
	table_file_header *table = NULL;
	tpd_entry *tpd_cur = NULL;
	token_list *cur;

	cur = t_list;
	
	if (cur->tok_class != identifier)
	{
		rc = INVALID_IMAGEFILE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			strcpy(image_file_name, cur->tok_string);
			
			if ((fhandle = fopen(image_file_name, "rbc")) == NULL)			// Image file does not exist
			{
				if ((fhandle = fopen(image_file_name, "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);
					for (i = 0, tpd_cur = &(g_tpd_list->tpd_start); i < g_tpd_list->num_tables; i++)
					{
						rc = read_table_from_file(strcat(tpd_cur->table_name, ".tab"), &table);
						if (!rc)
						{
							fwrite(&(table->file_size), sizeof(int), 1, fhandle);
							fwrite(table, table->file_size, 1, fhandle);
							free(table);
							tpd_cur = (tpd_entry*)((char*)tpd_cur + tpd_cur->tpd_size);
						}
						else
						{
							break;
						}						
					}
					fflush(fhandle);
					fclose(fhandle);

					if (!rc)
					{
						rc = write_to_log(image_file_name, BACKUP);
					}
				}
			}
			else														//Image file exists
			{
				printf("Image file name already exist\n");
				rc = DUPLICATE_IMAGEFILE_NAME;
				cur->tok_value = INVALID;
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}

	return rc;
}

int sem_restore(token_list *t_list)
{
	int rc = 0, i, new_list_size = 0, new_table_size = 0;
	char image_file_name[MAX_TOK_LEN + 1] = "", backup_tag[MAX_TOK_LEN] = "", temp[MAX_LOG_LEN] = "";
	char *image_ptr = NULL, *image_cur = NULL, *temp_log = NULL;
	bool rollforward = false;
	token_list *cur;
	FILE *fhandle = NULL;
	tpd_entry *tpd_cur = NULL;
	tpd_list *new_list = NULL;
	struct _stat file_stat;

	cur = t_list;

	if (cur->tok_class != identifier)
	{
		rc = INVALID_IMAGEFILE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		strcpy(image_file_name, cur->tok_string);
		cur = cur->next;

		if ((fhandle = fopen(image_file_name, "rbc")) == NULL)			//read image file
		{
			printf("Image file does not exist\n");
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			_fstat(_fileno(fhandle), &file_stat);
			image_ptr = (char*)calloc(1, file_stat.st_size);
			if (image_ptr == NULL)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				image_cur = image_ptr;
				fread(image_cur, file_stat.st_size, 1, fhandle);
				
				if (cur->tok_value != EOC)
				{
					if (cur->tok_value == K_WITHOUT && cur->next->tok_value == K_RF)					// WITHOUT RF
					{
						rc = prune_log(image_file_name);
					}
					else
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
					}
				}
				else																			// WITH RF
				{
					rollforward = true;
				}

				fflush(fhandle);
				fclose(fhandle);

				if (rollforward)
				{
					sprintf(backup_tag, "BACKUP %s\n", image_file_name);
					if ((fhandle = fopen("db.log", "r")) == NULL)
					{
						rc = FILE_OPEN_ERROR;
					}
					else
					{
						_fstat(_fileno(fhandle), &file_stat);
						temp_log = (char*)calloc(1, file_stat.st_size + sizeof("RF_START\n"));
						if (temp_log == NULL)
						{
							rc = MEMORY_ERROR;
						}
						else
						{
							while (fgets(temp, MAX_LOG_LEN, fhandle) != NULL && strcmp(temp, backup_tag) != 0)
							{
								strcat(temp_log, temp);
							}

							strcat(temp_log, backup_tag);
							strcat(temp_log, "RF_START\n");

							while (fgets(temp, MAX_LOG_LEN, fhandle) != NULL)
							{
								strcat(temp_log, temp);
							}

							fflush(fhandle);
							fclose(fhandle);

							if ((fhandle = fopen("db.log", "w")) == NULL)
							{
								rc = FILE_OPEN_ERROR;
							}
							else
							{
								fwrite(temp_log, strlen(temp_log), 1, fhandle);
								fflush(fhandle);
								fclose(fhandle);
							}
						}

						free(temp_log);
					}
				}

				if (!rc)	// retore from image file
				{
					for (i = 0, tpd_cur = &(g_tpd_list->tpd_start); i < g_tpd_list->num_tables; i++)		// delete all old tables
					{
						if (remove(strcat(tpd_cur->table_name, ".tab")) != 0)
							rc = DELETE_TABLE_ERROR;
						tpd_cur = (tpd_entry*)((char*)tpd_cur + tpd_cur->tpd_size);
					}

					free(g_tpd_list);																		// free old tpd_list

					memcpy(&new_list_size, image_cur, 4);
					g_tpd_list = (tpd_list*)calloc(1, new_list_size);
					if (!g_tpd_list)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy(g_tpd_list, image_cur, new_list_size);										// restore new list to ptr
						image_cur += new_list_size;

						if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
						else
						{
							if (rollforward)
								g_tpd_list->db_flags = ROLLFORWARD_PENDING;

							fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);
							for (i = 0, tpd_cur = &(g_tpd_list->tpd_start); i < g_tpd_list->num_tables; i++)
							{
								memcpy(&new_table_size, image_cur, 4);
								image_cur += 4;

								if ((fhandle = fopen(strcat(tpd_cur->table_name, ".tab"), "wbc")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
									break;
								}
								else
								{
									fwrite(image_cur, new_table_size, 1, fhandle);
									image_cur += new_table_size;
									fflush(fhandle);
									fclose(fhandle);
								}

								tpd_cur = (tpd_entry*)((char*)tpd_cur + tpd_cur->tpd_size);
							}
						}
					}
				}

				free(image_ptr);
			}
		}
	}

	return rc;
}

int sem_rollforward(token_list *t_list)
{
	int rc = 0, internal_rc = 0, i;
	char timesatmp[TIMESTAMP_LEN + 1] = "", temp[MAX_LOG_LEN] = "", temp2[MAX_LOG_LEN] = "";
	char *tok1 = NULL, *tok2 = NULL, *temp_log = NULL;
	bool rf_to_time = false, found_tag = false;
	token_list *cur, *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	cur = t_list;

	if (cur->tok_value != EOC)
	{
		if (cur->tok_value != K_TO)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			
			if (cur->tok_value != INT_LITERAL)
			{
				printf("Error, TIMESTAMP should all be digits\n");
				rc = INVALID_TIMESTAMP;
				cur->tok_value = INVALID;
			}

			rc = check_timestamp(cur->tok_string);
			if (rc)
				cur->tok_value = INVALID;
			else
			{
				strcpy(timesatmp, cur->tok_string);
				rf_to_time = true;
			}
		}
	}

	if (!rc)
	{
		if ((fhandle = fopen("db.log", "r")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			_fstat(_fileno(fhandle), &file_stat);
			temp_log = (char*)calloc(1, file_stat.st_size);
			if (temp_log == NULL)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				while (fgets(temp, MAX_LOG_LEN, fhandle) != NULL)
				{
					if (strcmp(temp, "RF_START\n") == 0)
					{
						found_tag = true;
						break;
					}
					else
					{
						strcat(temp_log, temp);
					}
				}

				if (!found_tag)
				{
					printf("Missing RF_START tag in db.log\n");
					rc = MISSING_RF_TAG;
				}
				else
				{
					g_tpd_list->db_flags = ROLLFORWARD_DONE;
					internal_command = true;				//set internal command mode on
					while (fgets(temp, MAX_LOG_LEN, fhandle) != NULL)
					{						
						tok1 = strtok(temp, " ");
						tok2 = strtok(NULL, "\n");

						if (strcmp(tok1, "BACKUP") != 0)
						{
							sprintf(temp2, "%s %s\n", tok1, tok2);
							if (rf_to_time && (strcmp(tok1, timesatmp) > 0))
							{
								break;
							}

							printf("Redo %s\n", tok2);
							tok2++;
							*(tok2 + strlen(tok2) - 1) = '\0';

							internal_rc = get_token(tok2, &tok_list);
							if (!internal_rc)
							{
								internal_rc = do_semantic(tok_list);
							}

							tok_ptr = tok_list;
							while (tok_ptr != NULL)
							{
								tmp_tok_ptr = tok_ptr->next;
								free(tok_ptr);
								tok_ptr = tmp_tok_ptr;
							}
							tok_list = NULL;
						}
						strcat(temp_log, temp2);
					}
				}
				fflush(fhandle);
				fclose(fhandle);

				if (!rc)		//update log records, removing the RF_START tag
				{
					if ((fhandle = fopen("db.log", "w")) == NULL)
					{
						rc = FILE_OPEN_ERROR;
					}
					else
					{
						//printf("temp log: %s\n", temp_log);
						fwrite(temp_log, strlen(temp_log), 1, fhandle);

						fflush(fhandle);
						fclose(fhandle);
					}
				}
			}
			
			free(temp_log);
		}

		if (!rc)		//update db_flag in bin file with ROLLFORWARD_DONE
		{
			if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);

				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}


	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

  /* Open for read */
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int read_table_from_file(char filename[], table_file_header **tab)
{
	int rc = 0;
	FILE *fhandle = NULL;
	table_file_header *table = NULL;
	struct _stat file_stat;

	if ((fhandle = fopen(filename, "rbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		table = (table_file_header*)calloc(1, file_stat.st_size);
		if (table == NULL)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(table, file_stat.st_size, 1, fhandle);

			if (table->file_size != file_stat.st_size)
			{
				printf("TABFILE_CORRUPTION\n");
				rc = TABFILE_CORRUPTION;
				free(table);
				table = NULL;
			}
		}

		fflush(fhandle);
		fclose(fhandle);
	}

	*tab = table;
	return rc;
}

int where_clause_handler(token_list **t_list, tpd_entry *tab_entry, table_file_header *table, char *records[])
{
	int rc = 0, relation = 0, col_record_offset, col_record_offset2, i, record_int, temp_int, record_int2, and_flag = 0;
	token_list *cur = *t_list;
	cd_entry *col_entry = NULL, *col_entry2 = NULL;
	cd_entry *col_cur = NULL;
	char int_max_string[10], temp[MAX_TOK_LEN * 2] = "", temp2[MAX_TOK_LEN * 2] = "";
	char *table_cur = NULL, *record_cur = NULL, *record_cur2 = NULL, *temp_record = NULL;

	do
	{
		cur = cur->next;

		rc = get_cd_from_tpd(&cur, tab_entry, &col_entry);

		if (!rc)
		{
			cur = cur->next;

			for (i = 0, col_record_offset = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				i < col_entry->col_id; i++, col_cur++, col_record_offset++)
			{
				col_record_offset += col_cur->col_len;
			}
			table_cur = ((char*)table + table->record_offset);

			if (cur->tok_value == K_IS)
			{
				cur = cur->next;

				if (cur->tok_value == K_NULL)
				{
					if (!col_entry->not_null)
					{
						for (i = 0; i < table->num_records; i++)
						{
							temp_record = records[i];
							if (and_flag)
							{
								if (records[i] != NULL)
								{
									records[i] = NULL;
								}
							}
							if (!(and_flag && temp_record == NULL))
							{
								record_cur = table_cur + col_record_offset;
								if (memcmp(record_cur, &null_byte, 1) == 0)
								{
									records[i] = table_cur;
								}
							}
							table_cur += table->record_size;
						}
					}
				}
				else
				{
					if (cur->tok_value == K_NOT)
					{
						cur = cur->next;
						if (cur->tok_value == K_NULL)
						{
							for (i = 0; i < table->num_records; i++)
							{
								temp_record = records[i];
								if (and_flag)
								{
									if (records[i] != NULL)
									{
										records[i] = NULL;
									}
								}
								if (!(and_flag && temp_record == NULL))
								{
									record_cur = table_cur + col_record_offset;
									if (memcmp(record_cur, &null_byte, 1) != 0)
									{
										records[i] = table_cur;
									}
								}
								table_cur += table->record_size;
							}
						}
						else
						{
							printf("INVALID_CONDITION error\n");
							rc = INVALID_CONDITION;
							cur->tok_value = INVALID;
						}
					}
					else
					{
						printf("INVALID_CONDITION error\n");
						rc = INVALID_CONDITION;
						cur->tok_value = INVALID;
					}
				}
			}
			else
			{
				if ((cur->tok_value != S_EQUAL) &&
					(cur->tok_value != S_LESS) &&
					(cur->tok_value != S_GREATER))
				{
					printf("INVALID_RELATION_OPERATOR error\n");
					rc = INVALID_RELATION_OPERATOR;
					cur->tok_value = INVALID;
				}
				else
				{
					relation = cur->tok_value;
					cur = cur->next;

					if (cur->tok_value != STRING_LITERAL && cur->tok_value != INT_LITERAL)
					{
						rc = get_cd_from_tpd(&cur, tab_entry, &col_entry2);
						if (!rc)
						{
							if (col_entry->col_type != col_entry2->col_type)
							{
								printf("column type mismatch\n");
								rc = TYPE_MISMATCH;
								cur->tok_value = INVALID;
							}
							else						// join predicate
							{
								for (i = 0, col_record_offset2 = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									i < col_entry2->col_id; i++, col_cur++, col_record_offset2++)
								{
									col_record_offset2 += col_cur->col_len;
								}

								if (col_entry->col_type == T_CHAR)
								{
									if (relation == S_GREATER)
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												record_cur2 = table_cur + col_record_offset2;
												if (memcmp(record_cur++, &null_byte, 1) != 0 && memcmp(record_cur2++, &null_byte, 1) != 0)
												{
													memset(&temp, '\0', MAX_TOK_LEN);
													memcpy((void*)&temp, (void*)record_cur, col_entry->col_len);
													memset(&temp2, '\0', MAX_TOK_LEN);
													memcpy((void*)&temp2, (void*)record_cur2, col_entry2->col_len);

													if (strcmp(temp, temp2) > 0)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
									else if (relation == S_LESS)
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												record_cur2 = table_cur + col_record_offset2;
												if (memcmp(record_cur++, &null_byte, 1) != 0 && memcmp(record_cur2++, &null_byte, 1) != 0)
												{
													memset(&temp, '\0', MAX_TOK_LEN);
													memcpy((void*)&temp, (void*)record_cur, col_entry->col_len);
													memset(&temp2, '\0', MAX_TOK_LEN);
													memcpy((void*)&temp2, (void*)record_cur2, col_entry2->col_len);

													if (strcmp(temp, temp2) < 0)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
									else
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												record_cur2 = table_cur + col_record_offset2;
												if (memcmp(record_cur++, &null_byte, 1) != 0 && memcmp(record_cur2++, &null_byte, 1) != 0)
												{
													memset(&temp, '\0', MAX_TOK_LEN);
													memcpy((void*)&temp, (void*)record_cur, col_entry->col_len);
													memset(&temp2, '\0', MAX_TOK_LEN);
													memcpy((void*)&temp2, (void*)record_cur2, col_entry2->col_len);

													if (strcmp(temp, temp2) == 0)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
								}
								else  //INT type
								{									
									if (relation == S_GREATER)
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												record_cur2 = table_cur + col_record_offset2;
												if (memcmp(record_cur++, &null_byte, 1) != 0 && memcmp(record_cur2++, &null_byte, 1) != 0)
												{
													memcpy((void*)&record_int, (void*)record_cur, col_entry->col_len);
													memcpy((void*)&record_int2, (void*)record_cur2, col_entry2->col_len);													
													if (record_int > record_int2)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
									else if (relation == S_LESS)
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												record_cur2 = table_cur + col_record_offset2;
												if (memcmp(record_cur++, &null_byte, 1) != 0 && memcmp(record_cur2++, &null_byte, 1) != 0)
												{
													memcpy((void*)&record_int, (void*)record_cur, col_entry->col_len);
													memcpy((void*)&record_int2, (void*)record_cur2, col_entry2->col_len);
													if (record_int < record_int2)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
									else
									{												
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												record_cur2 = table_cur + col_record_offset2;
												if (memcmp(record_cur++, &null_byte, 1) != 0 && memcmp(record_cur2++, &null_byte, 1) != 0)
												{
													memcpy((void*)&record_int, (void*)record_cur, col_entry->col_len);
													memcpy((void*)&record_int2, (void*)record_cur2, col_entry2->col_len);
													if (record_int == record_int2)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
								}
							}
						}
					}
					else // not join predicate
					{
						rc = 0;
						if (col_entry->col_type == T_CHAR)
						{
							if (cur->tok_value != STRING_LITERAL)
							{
								printf("type mismatch\n");
								rc = TYPE_MISMATCH;
								cur->tok_value = INVALID;
							}
							else
							{
								if (relation == S_GREATER)
								{
									for (i = 0; i < table->num_records; i++)
									{
										temp_record = records[i];
										if (and_flag)
										{
											if (records[i] != NULL)
											{
												records[i] = NULL;
											}
										}
										if (!(and_flag && temp_record == NULL))
										{
											record_cur = table_cur + col_record_offset;
											if (memcmp(record_cur++, &null_byte, 1) != 0)
											{
												memset(&temp, '\0', MAX_TOK_LEN);
												memcpy((void*)&temp, (void*)record_cur, col_entry->col_len);
												if (strcmp(temp, cur->tok_string) > 0)
												{
													records[i] = table_cur;
												}
											}
										}
										table_cur += table->record_size;
									}
								}
								else if (relation == S_LESS)
								{
									for (i = 0; i < table->num_records; i++)
									{
										temp_record = records[i];
										if (and_flag)
										{
											if (records[i] != NULL)
											{
												records[i] = NULL;
											}
										}
										if (!(and_flag && temp_record == NULL))
										{
											record_cur = table_cur + col_record_offset;
											if (memcmp(record_cur++, &null_byte, 1) != 0)
											{
												memset(&temp, '\0', MAX_TOK_LEN);
												memcpy((void*)&temp, (void*)record_cur, col_entry->col_len);

												if (strcmp(temp, cur->tok_string) < 0)
												{
													records[i] = table_cur;
												}
											}
										}
										table_cur += table->record_size;
									}
								}
								else
								{
									for (i = 0; i < table->num_records; i++)
									{
										temp_record = records[i];
										if (and_flag)
										{
											if (records[i] != NULL)
											{
												records[i] = NULL;
											}
										}
										if (!(and_flag && temp_record == NULL))
										{
											record_cur = table_cur + col_record_offset;
											if (memcmp(record_cur++, &null_byte, 1) != 0)
											{
												memset(&temp, '\0', MAX_TOK_LEN);
												memcpy((void*)&temp, (void*)record_cur, col_entry->col_len);
												if (strcmp(temp, cur->tok_string) == 0)
												{
													records[i] = table_cur;
												}
											}
										}
										table_cur += table->record_size;
									}
								}
							}

						}
						else     //must be T_INT
						{
							if (cur->tok_value != INT_LITERAL)
							{
								printf("type mismatch\n");
								rc = TYPE_MISMATCH;
								cur->tok_value = INVALID;
							}
							else
							{
								itoa(INT_MAX, int_max_string, 10);
								if ((strlen(cur->tok_string) > strlen(int_max_string)) ||
									((strlen(cur->tok_string) > strlen(int_max_string)) && (strcmp(cur->tok_string, int_max_string) > 0)))
								{
									rc = MAX_INT_EXCEEDED;
									cur->tok_value = INVALID;
								}
								else
								{
									temp_int = atoi(cur->tok_string);
									if (relation == S_GREATER)
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												if (memcmp(record_cur++, &null_byte, 1) != 0)
												{
													memcpy((void*)&record_int, (void*)record_cur, col_entry->col_len);
													if (record_int > temp_int)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
									else if (relation == S_LESS)
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												if (memcmp(record_cur++, &null_byte, 1) != 0)
												{
													memcpy((void*)&record_int, (void*)record_cur, col_entry->col_len);
													if (record_int < temp_int)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
									else
									{
										for (i = 0; i < table->num_records; i++)
										{
											temp_record = records[i];
											if (and_flag)
											{
												if (records[i] != NULL)
												{
													records[i] = NULL;
												}
											}
											if (!(and_flag && temp_record == NULL))
											{
												record_cur = table_cur + col_record_offset;
												if (memcmp(record_cur++, &null_byte, 1) != 0)
												{
													memcpy((void*)&record_int, (void*)record_cur, col_entry->col_len);
													if (record_int == temp_int)
													{
														records[i] = table_cur;
													}
												}
											}
											table_cur += table->record_size;
										}
									}
								}
							}
						}
					}					
				}
			}

			if (!rc)
			{
				cur = cur->next;

				if (cur->tok_value != EOC)
				{
					if (cur->tok_value == K_AND)
					{
						and_flag = 1;
					}
					else if (cur->tok_value == K_OR)
					{
						and_flag = 0;
					}
				}
			}
		}
	} while (!rc && (cur->tok_value == K_AND || cur->tok_value == K_OR));

	*t_list = cur;
	return rc;
}

int order_by_handler(token_list **t_list, tpd_entry *tab_entry, table_file_header *table, char *records[])
{
	int rc = 0, order = K_ASC, temp_int_1, temp_int_2, i, j, sort_num = 0, cd_record_offset;
	token_list *cur = *t_list;
	cd_entry *col_entry = NULL;
	cd_entry *col_cur = NULL;
	char temp_1[MAX_TOK_LEN] = "", temp_2[MAX_TOK_LEN] = "";
	char *sorted_records[MAX_NUM_RECORDS] = {NULL}, *temp = NULL;

	cur = cur->next;

	rc = get_cd_from_tpd(&cur, tab_entry, &col_entry);

	if (!rc)
	{
		cur = cur->next;

		for (i = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset), cd_record_offset = 0;
			i < col_entry->col_id; i++, col_cur++, cd_record_offset++)
		{
			cd_record_offset += col_cur->col_len;
		}

		if (cur->tok_value != K_DESC && cur->tok_value != K_ASC && cur->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			for (i = 0, j = 0; i < table->num_records; i++)
			{
				if (records[i] != NULL)
					sorted_records[j++] = records[i];
			}
			sort_num = j;


			if (col_entry->col_type == T_CHAR)
			{
				for (i = 1; i < sort_num; i++)
				{
					j = i;

					while (j > 0)
					{
						memset(&temp_1, '\0', MAX_TOK_LEN);
						memcpy((void*)&temp_1, (void*)(sorted_records[j] + cd_record_offset + 1), col_entry->col_len);
						memset(&temp_2, '\0', MAX_TOK_LEN);
						memcpy((void*)&temp_2, (void*)(sorted_records[j - 1] + cd_record_offset + 1), col_entry->col_len);

						if (cur->tok_value == K_DESC)
						{
							if (memcmp(sorted_records[j - 1] + cd_record_offset, &null_byte, 1) != 0)
							{
								if ((memcmp(sorted_records[j] + cd_record_offset, &null_byte, 1) == 0) || strcmp(temp_1, temp_2) > 0)
								{
									temp = sorted_records[j];
									sorted_records[j] = sorted_records[j - 1];
									sorted_records[j - 1] = temp;
									j--;
								}
								else
									break;
							}
							else
								break;
						}
						else
						{
							if (memcmp(sorted_records[j] + cd_record_offset, &null_byte, 1) != 0)
							{
								if ((memcmp(sorted_records[j - 1] + cd_record_offset, &null_byte, 1) == 0) || strcmp(temp_1, temp_2) < 0)
								{
									temp = sorted_records[j];
									sorted_records[j] = sorted_records[j - 1];
									sorted_records[j - 1] = temp;
									j--;
								}
								else
									break;
							}
							else
								break;
						}
					}
				}
			}
			else			// INT type
			{
				for (i = 1; i < sort_num; i++)
				{
					j = i;

					while (j > 0)
					{
						memcpy((void*)&temp_int_1, (void*)(sorted_records[j] + cd_record_offset + 1), col_entry->col_len);
						memcpy((void*)&temp_int_2, (void*)(sorted_records[j - 1] + cd_record_offset + 1), col_entry->col_len);

						if (cur->tok_value == K_DESC)
						{
							if (memcmp(sorted_records[j - 1] + cd_record_offset, &null_byte, 1) != 0)
							{
								if ((memcmp(sorted_records[j] + cd_record_offset, &null_byte, 1) == 0) || temp_int_1 > temp_int_2)
								{
									temp = sorted_records[j];
									sorted_records[j] = sorted_records[j - 1];
									sorted_records[j - 1] = temp;
									j--;
								}
								else
									break;
							}
							else
								break;
						}
						else
						{
							if (memcmp(sorted_records[j] + cd_record_offset, &null_byte, 1) != 0)
							{
								if ((memcmp(sorted_records[j - 1] + cd_record_offset, &null_byte, 1) == 0) || temp_int_1 < temp_int_2)
								{
									temp = sorted_records[j];
									sorted_records[j] = sorted_records[j - 1];
									sorted_records[j - 1] = temp;
									j--;
								}
								else
									break;
							}
							else
								break;
						}
					}
				}
			}

			for (i = 0; i < table->num_records; i++)
			{
				records[i] = NULL;
				records[i] = sorted_records[i];
			}

			if (cur->tok_value == K_ASC || cur->tok_value == K_DESC)
				cur = cur->next;
		}
	}
	
	*t_list = cur;
	return rc;
}

void print_table(cd_entry *col_array[], tpd_entry *tab_entry, int num_records, char *records[], int num_col)
{
	char boarder[MAX_TOK_LEN * MAX_NUM_COL * 2] = "+";
	char col_names[MAX_TOK_LEN * MAX_NUM_COL * 2] = "|";
	int label_len[MAX_NUM_COL * 2], col_name_len, i, j = 1, k, records_selected = 0, col_record_offset;
	cd_entry *col_entry = NULL;
	cd_entry *col_cur = NULL;
	char *table_cur = NULL, *record_cur = NULL;
	char temp[MAX_TOK_LEN * 2] = "";
	int temp_int = 0;
	float temp_float = 0;

	for (i = 0; i < num_col; i++)
	{
		col_name_len = strlen(col_array[i]->col_name);
		if (col_array[i]->col_type == T_CHAR)
		{
			if (col_array[i]->col_len > col_name_len)
			{
				label_len[i] = col_array[i]->col_len;
			}
			else
			{
				label_len[i] = col_name_len;
			}
		}
		else if (col_array[i]->col_type == T_INT)					
		{
			if (MAX_INT_STRING_LEN > col_name_len)
			{
				label_len[i] = MAX_INT_STRING_LEN;
			}
			else
			{
				label_len[i] = col_name_len;
			}
		}
		else																//must be float
		{
			if (MAX_FLOAT_STRING_LEN > col_name_len)
			{
				label_len[i] = MAX_FLOAT_STRING_LEN;
			}
			else
			{
				label_len[i] = col_name_len;
			}
		}

		strcat(col_names, col_array[i]->col_name);
		for (k = col_name_len; k < label_len[i] + 1; k++)					//pading col names with spaces
		{
			col_names[k + j] = ' ';
		}

		for (k = 0; k < label_len[i]; k++, j++)							//drawing boarder
		{
			boarder[j] = '-';
		}
		boarder[j] = '+';
		col_names[j++] = '|';
	}

	printf("%s\n", boarder);
	printf("%s\n", col_names);
	printf("%s\n", boarder);

	for (i = 0; i < num_records; i++)							//printing records
	{
		if (records[i] != NULL)
		{			
			printf("|");
			for (k = 0;	k < num_col; k++)
			{
				for (j = 0, col_record_offset = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
						j < col_array[k]->col_id; j++, col_record_offset++, col_cur++)
				{
					col_record_offset += col_cur->col_len;
				}

				record_cur = records[i] + col_record_offset;

				if (col_array[k]->col_type == T_CHAR)
				{
					if (memcmp(record_cur++, &null_byte, 1) == 0)						//NULL value
					{
						memset(&temp, '\0', MAX_TOK_LEN);
						strcat(temp, "-");
						printf("%-*s|", label_len[k], temp);
					}
					else
					{
						memset(&temp, '\0', MAX_TOK_LEN);
						memcpy((void*)&temp, (void*)record_cur, col_array[k]->col_len);
						printf("%-*s|", label_len[k], temp);
					}
				}
				else if (col_array[k]->col_type == T_INT)
				{
					if (!memcmp(record_cur++, &null_byte, 1))						//NULL value
					{
						memset(&temp, '\0', MAX_TOK_LEN);
						strcat(temp, "-");
						printf("%*s|", label_len[k], temp);
					}
					else
					{
						temp_int = 0;
						memcpy((void*)&temp_int, (void*)record_cur, col_array[k]->col_len);
						printf("%*d|", label_len[k], temp_int);
					}
				}
				else										//must be float
				{
					if (!memcmp(record_cur++, &null_byte, 1))						//NULL value
					{
						memset(&temp, '\0', MAX_TOK_LEN);
						strcat(temp, "-");
						printf("%*s|", label_len[k], temp);
					}
					else
					{
						temp_float = 0;
						memcpy((void*)&temp_float, (void*)record_cur, col_array[k]->col_len);
						printf("%*.2f|", label_len[k], temp_float);
					}
				}
			}
			printf("\n");
			records_selected++;
		}
	}
	printf("%s\n", boarder);
	printf("%d rows selected.\n", records_selected);
}

int func_count(cd_entry *col_entry, tpd_entry *tab_entry, table_file_header *table, char *records[], cd_entry *group_by_col)
{
	cd_entry count_col, *col_array[2] = {NULL}, *col_cur = NULL;
	tpd_entry temp_tpd, *temp_tpd_ptr = NULL;
	int i, j, count = 0, col_record_offset, group_by_col_offset, num_col = 1, rc = 0, num_records = 0, record_size = 0, count_record_offset = 0;
	char *group_by_records[MAX_NUM_RECORDS] = { NULL };
	bool found = false;

	if (group_by_col)
	{
		cd_entry group_col;										// creating group column
		group_col.col_id = 0;
		group_col.col_len = group_by_col->col_len;
		strcpy(group_col.col_name, group_by_col->col_name);
		group_col.col_type = group_by_col->col_type;
		group_col.not_null = group_by_col->not_null;
		col_array[num_col - 1] = &group_col;
		num_col++;

		count_record_offset += group_by_col->col_len + 2;
		rc = group_by_values(group_by_col, tab_entry, table, records, group_by_records, &num_records, &group_by_col_offset);
	}

	if (!rc)
	{
		memset(&count_col, '\0', sizeof(cd_entry));		// creating the count column
		count_col.col_id = num_col - 1;
		count_col.col_len = sizeof(int);
		count_col.col_type = T_INT;
		count_col.not_null = true;

		col_array[num_col - 1] = &count_col;

		temp_tpd.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	num_col;
		temp_tpd.cd_offset = sizeof(tpd_entry);
		temp_tpd.num_columns = num_col;
		temp_tpd_ptr = (tpd_entry*)calloc(1, temp_tpd.tpd_size);

		if (temp_tpd_ptr == NULL)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			memcpy((void*)temp_tpd_ptr, (void*)&temp_tpd, sizeof(tpd_entry));

			for (i = 0; i < num_col; i++, record_size++)
			{
				memcpy((void*)((char*)temp_tpd_ptr + sizeof(tpd_entry) + (i * sizeof(cd_entry))), (void*)col_array[i], sizeof(cd_entry));
				record_size += col_array[i]->col_len;
			}
		}

		if (!rc)
		{
			if (col_entry == NULL)
			{
				strcpy(count_col.col_name, "COUNT(*)");

				if (group_by_col)
				{
					for (i = 0; i < table->num_records; i++)
					{
						if (records[i] != NULL)
						{
							for (j = 0; j < num_records; j++)
							{
								if (memcmp(group_by_records[j], records[i] + group_by_col_offset, group_by_col->col_len + 1) == 0)
								{
									count = 0;
									memcpy(&count, group_by_records[j] + count_record_offset, sizeof(int));									
									count++;
									memcpy(group_by_records[j] + count_record_offset, &count, sizeof(int));
									break;
								}
							}
						}
					}
				}
				else
				{
					for (i = 0; i < table->num_records; i++)
					{
						if (records[i] != NULL)
						{
							count++;
						}
					}
				}
			}
			else
			{
				sprintf(count_col.col_name, "COUNT(%s)", col_entry->col_name);

				for (i = 0, col_record_offset = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					i < col_entry->col_id; i++, col_cur++, col_record_offset++)
				{
					col_record_offset += col_cur->col_len;
				}

				if (group_by_col)
				{
					for (i = 0; i < table->num_records; i++)
					{
						if (records[i] != NULL && memcmp(records[i] + col_record_offset, &null_byte, 1) != 0)
						{
							for (j = 0; j < num_records; j++)
							{
								if (memcmp(group_by_records[j], records[i] + group_by_col_offset, group_by_col->col_len + 1) == 0)
								{
									memcpy(&count, group_by_records[j] + count_record_offset, sizeof(int));
									count++;
									memcpy(group_by_records[j] + count_record_offset, &count, sizeof(int));
									break;
								}
							}
						}
					}
				}
				else
				{
					for (i = 0; i < table->num_records; i++)
					{
						if (records[i] != NULL && memcmp(records[i] + col_record_offset, &null_byte, 1) != 0)
						{
							count++;
						}
					}
				}
			}

			if (!group_by_col)
			{
				num_records = 1;
				group_by_records[0] = (char*)calloc(1, record_size);

				if (group_by_records[0] == NULL)
				{
					rc = MEMORY_ERROR;
				}
				else
				{
					memcpy((void*)group_by_records[0], &count_col.col_len, 1);
					memcpy((void*)(group_by_records[0] + 1), &count, sizeof(int));

					print_table(col_array, temp_tpd_ptr, num_records, group_by_records, num_col);

					free(group_by_records[0]);
				}
			}
			else
			{
				print_table(col_array, temp_tpd_ptr, num_records, group_by_records, num_col);

				for (i = 0; i < num_records; i++)
				{
					free(group_by_records[i]);
				}
			}

			free(temp_tpd_ptr);
		}
	}

	return rc;
}

int func_sum_avg(cd_entry *col_entry, tpd_entry *tab_entry, table_file_header *table, char *records[], int operation, cd_entry *group_by_col)
{
	cd_entry result_col, *col_array[2] = { NULL }, *col_cur = NULL;
	tpd_entry temp_tpd, *temp_tpd_ptr = NULL;
	int i, j, sum[MAX_NUM_RECORDS] = { 0 }, col_record_offset, rc = 0, count[MAX_NUM_RECORDS] = { 0 }, temp_int, group_by_col_offset, num_col = 1, num_records = 0, record_size = 0, result_offset = 0;
	char *group_by_records[MAX_NUM_RECORDS] = { NULL };
	bool found = false;
	float result;

	if (col_entry->col_type != T_INT)
	{
		printf("This function can only be used on integer type columns\n");
		rc = INVALID_AGGREGATE;
	}
	else
	{
		if (group_by_col)
		{
			cd_entry group_col;										// creating group column
			group_col.col_id = 0;
			group_col.col_len = group_by_col->col_len;
			strcpy(group_col.col_name, group_by_col->col_name);
			group_col.col_type = group_by_col->col_type;
			group_col.not_null = group_by_col->not_null;
			col_array[num_col - 1] = &group_col;
			num_col++;

			result_offset += group_by_col->col_len + 2;			//use to point to the result after adding to group_by_records[i]
			rc = group_by_values(group_by_col, tab_entry, table, records, group_by_records, &num_records, &group_by_col_offset);
		}

		if (!rc)
		{
			memset(&result_col, '\0', sizeof(cd_entry));		// creating the result column
			result_col.col_id = num_col - 1;
			result_col.not_null = false;
			if (operation == F_SUM)
			{
				result_col.col_len = sizeof(int);
				result_col.col_type = T_INT;
			}
			else
			{
				result_col.col_len = sizeof(float);
				result_col.col_type = T_FLOAT;
			}

			col_array[num_col - 1] = &result_col;

			temp_tpd.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	num_col;
			temp_tpd.cd_offset = sizeof(tpd_entry);
			temp_tpd.num_columns = num_col;
			temp_tpd_ptr = (tpd_entry*)calloc(1, temp_tpd.tpd_size);

			if (temp_tpd_ptr == NULL)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				memcpy((void*)temp_tpd_ptr, (void*)&temp_tpd, sizeof(tpd_entry));

				for (i = 0; i < num_col; i++, record_size++)
				{
					memcpy((void*)((char*)temp_tpd_ptr + sizeof(tpd_entry) + (i * sizeof(cd_entry))), (void*)col_array[i], sizeof(cd_entry));
					record_size += col_array[i]->col_len;
				}
			}

			if (!rc)
			{
				if (operation == F_SUM)
					sprintf(result_col.col_name, "SUM(%s)", col_entry->col_name);
				else
					sprintf(result_col.col_name, "AVG(%s)", col_entry->col_name);

				for (i = 0, col_record_offset = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					i < col_entry->col_id; i++, col_cur++, col_record_offset++)
				{
					col_record_offset += col_cur->col_len;
				}

				if (group_by_col)
				{
					for (i = 0; i < table->num_records; i++)
					{
						if (records[i] != NULL && memcmp(records[i] + col_record_offset, &null_byte, 1) != 0)
						{
							for (j = 0; j < num_records; j++)
							{
								if (memcmp(group_by_records[j], records[i] + group_by_col_offset, group_by_col->col_len + 1) == 0)
								{
									memcpy((void*)&temp_int, (void*)(records[i] + col_record_offset + 1), col_entry->col_len);
									sum[j] += temp_int;
									count[j]++;
									break;
								}
							}
						}
					}

					for (j = 0; j < num_records; j++)
					{
						if (count[j] == 0) // if count is 0
						{
							memset(group_by_records[j] + result_offset - 1, '\0', 1);
						}
						else
						{
							if (operation == F_SUM)
							{
								memcpy(group_by_records[j] + result_offset, &sum[j], sizeof(int));
							}
							else
							{
								result = sum[j];
								result /= count[j];
								memcpy(group_by_records[j] + result_offset, &result, sizeof(float));
							}
						}
					}										
				}
				else
				{
					for (i = 0; i < table->num_records; i++)
					{
						if (records[i] != NULL && memcmp(records[i] + col_record_offset, &null_byte, 1) != 0)
						{
							memcpy((void*)&temp_int, (void*)(records[i] + col_record_offset + 1), col_entry->col_len);
							sum[0] += temp_int;
							count[0]++;
						}
					}

					num_records = 1;
					group_by_records[0] = (char*)calloc(1, record_size);

					if (group_by_records[0] == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						if (count[0] == 0) // if count is 0
						{
							memset(group_by_records[0], '\0', 1);
						}
						else
						{
							memcpy(group_by_records[0], &result_col.col_len, 1);

							if (operation == F_SUM)
							{
								memcpy(group_by_records[0] + 1, &sum[0], sizeof(int));
							}
							else
							{
								result = sum[0];
								result /= count[0];
								memcpy(group_by_records[0] + 1, &result, sizeof(float));
							}
						}						
					}					
				}

				if (!rc)
				{
					print_table(col_array, temp_tpd_ptr, num_records, group_by_records, num_col);

					if (group_by_col)
					{
						for (i = 0; i < num_records; i++)
						{
							free(group_by_records[i]);
						}
					}
					else
					{
						free(group_by_records[0]);
					}
				}
				free(temp_tpd_ptr);
			}
		}
	}

	return rc;
}

int get_cd_from_tpd(token_list **t_list, tpd_entry *tab_entry, cd_entry **col_entry)
{
	int rc = 0, num_col = tab_entry->num_columns;
	token_list *cur = *t_list;
	cd_entry *col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
	bool found = false;

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		rc = INVALID_COLUMN_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		while ((!found) && (num_col-- > 0))
		{
			if (stricmp(col_cur->col_name, cur->tok_string) == 0)
			{
				found = true;
				*col_entry = col_cur;
			}
			else
			{
				if (num_col > 0)
				{
					col_cur++;
				}
			}
		}

		if (!found)
		{
			printf("COLUMN_NOT_EXIST error\n");
			rc = COLUMN_NOT_EXIST;
			cur->tok_value = INVALID;
		}
	}

	return rc;
}

int write_to_log(char *command, int com_type)
{
	int rc = 0;
	char timestamp[TIMESTAMP_LEN + 1] = "", log_string[MAX_LOG_LEN] = "";
	FILE *fhandle = NULL;
	struct _stat file_stat;
	time_t rawtime;
	struct tm * timeinfo;

	if (com_type != BACKUP && com_type != RESTORE)
	{ 
		time(&rawtime);
		timeinfo = localtime(&rawtime);

		sprintf(timestamp, "%d%02d%02d%02d%02d%02d",
			1900 + timeinfo->tm_year,
			timeinfo->tm_mon + 1,
			timeinfo->tm_mday,
			timeinfo->tm_hour,
			timeinfo->tm_min,
			timeinfo->tm_sec
			);

		strcat(log_string, timestamp);
		sprintf(log_string, "%s \"%s\"\n", timestamp, command);
	}	
	else if (com_type == RESTORE)
	{
		strcpy(log_string, "RF_START\n");
	}
	else if (com_type == BACKUP)
	{
		sprintf(log_string, "BACKUP %s\n", command);
	}

	if ((fhandle = fopen("db.log", "a+tc")) == NULL)
	{
		printf("can't wrtie to log\n");
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		fwrite(log_string, strlen(log_string), 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int prune_log(char *image_name)
{
	int rc = 0, log_number = 1;
	char backup_log_name[MAX_TOK_LEN] = "", temp[MAX_LOG_LEN] = "", backup_tag[MAX_TOK_LEN] = "";
	char *temp_log = NULL;
	FILE *fhandle = NULL, *fhandle2 = NULL;
	struct _stat file_stat;

	if ((fhandle = fopen("db.log", "r")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		temp_log = (char*)calloc(1, file_stat.st_size);
		if (temp_log == NULL)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(temp_log, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			do
			{
				sprintf(backup_log_name, "db.log%d", log_number++);
			} while ((fhandle = fopen(backup_log_name, "r")) != NULL);

			if ((fhandle = fopen(backup_log_name, "w")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				fwrite(temp_log, strlen(temp_log), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);

				if ((fhandle = fopen(backup_log_name, "r")) == NULL || (fhandle2 = fopen("db.log", "w")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					sprintf(backup_tag, "BACKUP %s\n", image_name);
					while (fgets(temp, MAX_LOG_LEN, fhandle) != NULL && strcmp(temp, backup_tag) != 0)
					{
						fwrite(temp, strlen(temp), 1, fhandle2);
					}
					fwrite(temp, strlen(temp), 1, fhandle2);

					fflush(fhandle);
					fclose(fhandle);
					fflush(fhandle2);
					fclose(fhandle2);
				}
			}
			free(temp_log);
		}
	}

	return rc;
}

int check_timestamp(char *time_string)
{
	int rc = 0, i, year, mon, day, hour, min, sec;
	char timesatmp[TIMESTAMP_LEN + 1] = "", temp[2] = "";

	if (strlen(time_string) != TIMESTAMP_LEN)
	{
		printf("Error, TIMESTAMP length is incorrect\n");
		rc = INVALID_TIMESTAMP;
	}
	else
	{
		strcpy(timesatmp, time_string);

		if (!rc)
		{
			memcpy(temp, &timesatmp[4], 2);
			mon = atoi(temp);
			memcpy(temp, &timesatmp[6], 2);
			day = atoi(temp);
			memcpy(temp, &timesatmp[8], 2);
			hour = atoi(temp);
			memcpy(temp, &timesatmp[10], 2);
			min = atoi(temp);
			memcpy(temp, &timesatmp[12], 2);
			sec = atoi(temp);
			if (mon > 11 || day > 31 || hour > 23 || min > 59 || sec > 59)
			{
				printf("TIMESTAMP is invalid, please check again\n");
				rc = INVALID_TIMESTAMP;
			}
		}
	}

	return rc;
}

int group_by_values(cd_entry *group_by_col, tpd_entry *tab_entry, table_file_header *table, char *records[], char *group_by_records[], int *num_values, int *offset)
{
	int i, j, values = 0, group_by_col_offset, rc = 0, record_size = 0, int_size;
	cd_entry *col_cur = NULL;
	bool found = false;
	char testbuff = '\0';
	int testsize = 0, testsize2 = 0;

	int_size = sizeof(int);

	for (i = 0, group_by_col_offset = 0, col_cur = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		i < group_by_col->col_id; i++, col_cur++, group_by_col_offset++)
	{
		group_by_col_offset += col_cur->col_len;
	}

	record_size += group_by_col->col_len;
	record_size++;
	record_size += int_size;
	record_size++;

	for (i = 0; i < table->num_records; i++)
	{
		if (records[i] != NULL)
		{
			found = false;
			for (j = 0; j < values; j++)
			{
				if (memcmp(group_by_records[j], records[i] + group_by_col_offset, group_by_col->col_len + 1) == 0)
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				values++;

				group_by_records[j] = (char*)calloc(1, record_size);

				if (group_by_records[j] == NULL)
				{
					printf("MEMORY_ERROR\n");
					rc = MEMORY_ERROR;
					break;
				}
				else
				{
					memcpy((void*)group_by_records[j], records[i] + group_by_col_offset, 1);
					memcpy((void*)(group_by_records[j] + 1), records[i] + group_by_col_offset + 1, group_by_col->col_len);
					memcpy((void*)(group_by_records[j] + 1 + group_by_col->col_len), &int_size, 1);
				}
			}
		}
	}

	if (!rc) //return values by reference
	{
		*num_values = values;
		*offset = group_by_col_offset;
	}

	return rc;
}

int join_table(tpd_entry **tab_entry, tpd_entry *tab_entry2, table_file_header **table, table_file_header *table2)
{
	int rc = 0, i, j, cur_id = 0, trim_record_size1 = 0, trim_record_size2 = 0;
	tpd_entry *tab_entry1 = *tab_entry, *join_tpd_ptr = NULL, join_tpd;
	table_file_header *table1 = *table, *join_table_ptr = NULL, join_table;
	cd_entry *col_cur = NULL;
	char *join_table_cur = NULL, *table1_cur = NULL, *table2_cur = NULL;
	char *record_cur1 = NULL, *record_cur2 = NULL, *join_record;

	memset(&join_tpd, '\0', sizeof(tpd_entry));
	join_tpd.num_columns = tab_entry1->num_columns + tab_entry2->num_columns;
	join_tpd.cd_offset = sizeof(tpd_entry);
	join_tpd.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) * join_tpd.num_columns;

	join_tpd_ptr = (tpd_entry*)calloc(1, join_tpd.tpd_size);

	if (join_tpd_ptr == NULL)
	{
		rc = MEMORY_ERROR;
	}
	else
	{
		for (i = 0, col_cur = (cd_entry*)((char*)tab_entry1 + tab_entry1->cd_offset); i < tab_entry1->num_columns; i++, col_cur++)
		{
			trim_record_size1 += col_cur->col_len + 1;
		}

		for (i = 0, cur_id = tab_entry1->num_columns, col_cur = (cd_entry*)((char*)tab_entry2 + tab_entry2->cd_offset); i < tab_entry2->num_columns; i++, col_cur++, cur_id++)
		{
			col_cur->col_id = cur_id;
			trim_record_size2 += col_cur->col_len + 1;
		}

		memcpy(join_tpd_ptr, &join_tpd, sizeof(tpd_entry));
		memcpy((char*)join_tpd_ptr + sizeof(tpd_entry), (char*)tab_entry1 + tab_entry1->cd_offset, tab_entry1->tpd_size - sizeof(tpd_entry));
		memcpy((char*)join_tpd_ptr + tab_entry1->tpd_size, (char*)tab_entry2 + tab_entry2->cd_offset, tab_entry2->tpd_size - sizeof(tpd_entry));
	}

	if (!rc)
	{
		memset(&join_table, '\0', sizeof(table_file_header));
		join_table.num_records = table1->num_records * table2->num_records;
		join_table.record_size = trim_record_size1 + trim_record_size2;
		join_table.record_offset = sizeof(table_file_header);
		join_table.file_size = sizeof(table_file_header) + join_table.num_records * join_table.record_size;

		join_table_ptr = (table_file_header*)calloc(1, join_table.file_size);
		
		if (join_table_ptr == NULL)
		{
			rc = MEMORY_ERROR;			
		}
		else
		{
			memcpy(join_table_ptr, &join_table, sizeof(table_file_header));
			join_table_cur = (char*)join_table_ptr + join_table_ptr->record_offset;

			for (i = 0, table1_cur = (char*)table1 + table1->record_offset; i < table1->num_records; i++)
			{
				record_cur1 = table1_cur;

				for (j = 0, table2_cur = (char*)table2 + table2->record_offset; j < table2->num_records; j++)
				{
					record_cur2 = table2_cur;

					memcpy(join_table_cur, record_cur1, trim_record_size1);
					memcpy(join_table_cur + trim_record_size1, record_cur2, trim_record_size2);

					join_table_cur += join_table.record_size;
					table2_cur += table2->record_size;
				}

				table1_cur += table1->record_size;
			}
		}
	}

	if (!rc)
	{
		*tab_entry = join_tpd_ptr;
		*table = join_table_ptr;		
	}

	return rc;
}