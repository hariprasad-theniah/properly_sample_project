from os import environ
import datetime
import psycopg2
from psycopg2 import sql
from time import sleep
from json import loads
import sys
import re

class db_sync:
    etl_conn = None
    isautocommit = False
    date_ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    def connect(self):
        try:
            print(environ)
            credentials = {iDictK99:iDictV99 for iDictK99, iDictV99 in environ.items() if len(re.findall('(^POSTGRES_)',iDictK99)) > 0}
            print(credentials)
            connection_credentials = {}
            connection_credentials['host']            = credentials['POSTGRES_HOST']
            connection_credentials['port']            = 5432
            connection_credentials['database']        = credentials['POSTGRES_DB']
            connection_credentials['user']            = credentials['POSTGRES_USER']
            connection_credentials['password']        = credentials['POSTGRES_PASSWORD']
            connection_credentials['connect_timeout'] = 5
            self.etl_conn = psycopg2.connect(**connection_credentials)
            self.etl_conn.autocommit = self.isautocommit
            tmp_db_cur = self.etl_conn.cursor()
            tmp_db_cur.execute("select current_schema() as current_schema;")
            if tmp_db_cur.rowcount > 0:
                self.current_schema = tmp_db_cur.fetchone()[0]
            print("Current Schema " + self.current_schema)
            tmp_db_cur.close()
        except psycopg2.DatabaseError as e:
            raise
        except:
            raise
    def ExecuteQuery(self,pQueryString, pGetColumnNames=False):
        try:
            commit_chk_str = '^((copy)|(drop)|(create)|(alter)|(insert)|(update)|(delete)|(truncate))'
            non_commit_chk_str = '^((select)|(with))'
            tmp_db_cur = self.etl_conn.cursor()
            if isinstance(pQueryString, str):
                pQueryStringL = pQueryString
                pQueryString = (pQueryString,)
            elif isinstance(pQueryString, psycopg2.sql.Composed):
                pQueryStringL = pQueryString.as_string(self.etl_conn)
                pQueryString = (pQueryString,)
            elif isinstance(pQueryString, list) or isinstance(pQueryString, tuple):
                if len(pQueryString) > 2:
                    print("[WARNING]:Query String List/Tuple has more than required # of elements, The Method will consider only first 2 elements !")
                    pQueryString = (pQueryString[0],pQueryString[1])
                elif len(pQueryString) == 0:
                    raise Exception("[ERROR]:Query String List/Tuple has no element to execute !")
                if isinstance(pQueryString[0],psycopg2.sql.Composed):
                    pQueryStringL = pQueryString[0].as_string(self.etl_conn)
                else:
                    pQueryStringL = pQueryString[0]
            else:
                raise InvalidParameter(None,"[ERROR]:Query String can only be a String or ComposedSQL or List or Tuple, Other Types are not yet implemented !")
            print("INFO: Executing Query [{:s}]".format(pQueryStringL))
            tmp_db_cur.execute(*pQueryString)
            commit_chk_str_list = re.findall(commit_chk_str,pQueryStringL.strip().lower())
            non_commit_chk_str_list = re.findall(non_commit_chk_str,pQueryStringL.strip().lower())
            if len(commit_chk_str_list) > 0:
                self.etl_conn.commit()
                print("INFO: Query finished execution with message - " + tmp_db_cur.statusmessage)
                yield ([iL for iL in commit_chk_str_list[0] if iL.strip() != ""][0],tmp_db_cur.statusmessage)
            elif len(non_commit_chk_str_list) > 0:
                if pGetColumnNames:
                    yield [iColumnObj0.name for iColumnObj0 in tmp_db_cur.description]
                if tmp_db_cur.rowcount > 0:
                    while True:
                        row0 = tmp_db_cur.fetchone()
                        if row0 != None:
                            yield row0
                        else:
                            return
                else:
                    print("INFO: No records returned for the given query, Status Message[{:s}]".format(tmp_db_cur.statusmessage))
                    return
            else:
                raise Exception("[ERROR]:Query is not usual SQL, The query should start with any of the following word select, with, create, alter, insert, update, delete or truncate ...")
        except:
            raise
    def DropTable(self,pTName):
        try:
            if pTName == None or pTName.strip() == "":
                raise Exception(" The Table name parameter cannot be blank !")
            else:
                tschema = psycopg2.sql.Identifier(pTName.split('.')[0] if len(pTName.split('.')) > 1 else 'public') 
                ttablename = psycopg2.sql.Identifier(pTName.split('.')[1] if len(pTName.split('.')) > 1 else pTName.split('.')[0])
                table_check_query = psycopg2.sql.SQL("drop table if exists {0}.{1}").format(tschema,ttablename)
                tmp_rv = ()
                for iG in self.ExecuteQuery(table_check_query):
                    tmp_rv = iG[1]
                return tmp_rv
        except:
            raise
    def CopyFileToTable(self, **kwargs):
        try:
            tmp_db_cur = self.etl_conn.cursor()
            copy_query = f"copy {kwargs['STAGE_TABLENAME']} from stdin with (format 'csv', NULL '', QUOTE '\"')"
            inputfile = open(kwargs['INPUT_FILE'], 'r')
            tmp_db_cur.copy_expert(copy_query, inputfile)
        except:
            raise
    def execute_sqls(self, pSQLfile):
        try:
            with open(pSQLfile, 'r') as infile_ref:
                sqls = infile_ref.read()
                sql_list = sqls.split(';')
            for iL0 in sql_list:
                if iL0.strip() == '':
                    continue
                for iRs in self.ExecuteQuery(iL0):
                    print(iRs)
        except:
            raise
    def sync_table(self, **kwargs):
        try:
            staging_table = f"{kwargs['TABLENAME']}_{self.date_ts}"
            kwargs['STAGE_TABLENAME'] = staging_table
            for iRs in self.ExecuteQuery(f"create table {staging_table}(like {kwargs['TABLENAME']})"):
                print(iRs)
            for iL0 in kwargs['FILES']:
                kwargs['INPUT_FILE'] = iL0
                self.CopyFileToTable(**kwargs)
            if 'PRIMARYKEYS' in kwargs:
                join_condition = ' and '.join([f"act.{iL0} = ref.{iL0}" for iL0 in kwargs['PRIMARYKEYS']])
                where_condition = ' and '.join([f"ref.{iL0} isnull" for iL0 in kwargs['PRIMARYKEYS']])
                insert_query = (f"insert into {kwargs['TABLENAME']} "
                                f"select act.* from {kwargs['STAGE_TABLENAME']} act "
                                f"left join {kwargs['TABLENAME']} ref on {join_condition} "
                                f"where {where_condition}")
            else:
                insert_query = (f"insert into {kwargs['TABLENAME']} "
                                f"select * from {kwargs['STAGE_TABLENAME']} act ")
            for iRs in self.ExecuteQuery(insert_query):
                print(iRs)
            self.DropTable(staging_table)
        except:
            self.DropTable(staging_table)
            raise
    def dedup_table(self, **kwargs):
        try:
            keys_table = f"dedup_keys_{kwargs['TABLENAME']}_{self.date_ts}"
            records_table = f"dedup_records_{kwargs['TABLENAME']}_{self.date_ts}"
            group_by_cols = ','.join(kwargs['PRIMARYKEYS'])
            keys_table_sql = (
                                f"create table {keys_table} as "
                                f"select {group_by_cols}, count(*) as dup_count from {kwargs['TABLENAME']} group by {group_by_cols}"
                             )
            for iRs in self.ExecuteQuery(keys_table_sql):
                print(iRs)
            dup_count = 0
            for iRs in self.ExecuteQuery(f"select count(*) from {keys_table} where dup_count > 1"):
                dup_count = iRs[0]
            if dup_count > 0:
                print(f"INFO: No duplicates in table [{kwargs['TABLENAME']}] for keys [{group_by_cols}]")
                self.DropTable(keys_table)
                return
            delete_unq_keys_sql = (f"delete from {keys_table} where dup_count = 1")
            for iRs in self.ExecuteQuery(delete_unq_keys_sql):
                print(iRs)
            join_conditions = ' and '.join([f"act.{iL0} = ref.{iL0}" for iL0 in kwargs['PRIMARYKEYS']])
            partition_by_cols = ','.join([f"act.{iL0}" for iL0 in kwargs['PRIMARYKEYS']])
            recs_table_sql = (
                                f"create table {records_table} as "
                                f"select act.*, row_number() over(partition by {partition_by_cols} order by act.etl_extract_timestamp) as row_num from {kwargs['TABLENAME']} act "
                                f" join {keys_table} ref on {join_conditions}"
                             )
            for iRs in self.ExecuteQuery(recs_table_sql):
                print(iRs)
            join_conditions = ' and '.join([f"{kwargs['TABLENAME']}.{iL0} = ref.{iL0}" for iL0 in kwargs['PRIMARYKEYS']])
            delete_dup_keys_sql = (f"delete from {kwargs['TABLENAME']} using {keys_table} ref where {join_conditions}")
            for iRs in self.ExecuteQuery(delete_dup_keys_sql):
                print(iRs)
            get_table_columns = f"select column_name from information_schema.columns where table_schema = 'public' and table_name = '{kwargs['TABLENAME']}' order by ordinal_position"
            columns = []
            for iRs in self.ExecuteQuery(get_table_columns):
                columns.append(iRs[0])
            columns = ','.join(columns)
            insert_into_sql = (f"insert into {kwargs['TABLENAME']}({columns})"
                               f"select {columns} from {records_table} where row_num = 1"
                              )
            for iRs in self.ExecuteQuery(insert_into_sql):
                print(iRs)
            self.DropTable(keys_table)
            self.DropTable(records_table)
        except:
            self.DropTable(keys_table)
            self.DropTable(records_table)
            raise
if __name__ == '__main__':
    obj = db_sync()
    obj.connect()
    # obj.execute_sqls("./database_ddl.sql")
    obj.CopyFileToTable()
