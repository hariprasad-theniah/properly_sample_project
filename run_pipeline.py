from os import path
import pipeline.extract_api_source as extract_api_source
import pipeline.extract_kaggle_src as extract_kaggle_src
import pipeline.sync_to_database as sync_to_database

if __name__ == '__main__':
    kaggle_obj = extract_kaggle_src.refresh()
    kaggle_obj.initiate()
    db_obj = sync_to_database.db_sync()
    db_obj.connect()
    if not path.isfile("./.is_initialized"):
        kaggle_obj.execute_system_command(['touch', './.is_initialized'])
        db_obj.execute_sqls("./pipeline/database_ddl.sql")
    if len(kaggle_obj.list_of_output_files) > 0:
        kaggle_load_params = {}
        kaggle_load_params['TABLENAME'] = 'kaggle_dataset'
        kaggle_load_params['FILES'] = kaggle_obj.list_of_output_files
        db_obj.sync_table(**kaggle_load_params)
    kaggle_dedup_params = {}
    kaggle_dedup_params['TABLENAME'] = 'kaggle_dataset'
    kaggle_dedup_params['PRIMARYKEYS'] = ['id']
    # API Source
    api_obj = extract_api_source.refresh()
    api_obj.initiate()
    db_obj.dedup_table(**kaggle_dedup_params)
    api_load_params = {}
    api_load_params['TABLENAME'] = 'king_county_transit_dataset'
    api_load_params['FILES'] = ['./output_data/processed_data/transit_api_data.csv']
    api_load_params['PRIMARYKEYS'] = ['OBJECTID']
    db_obj.sync_table(**api_load_params)

