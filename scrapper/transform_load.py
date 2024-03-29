import pandas as pd
import json
import logging
import os

from sqlalchemy import create_engine
from datetime import datetime as dt
from constants import (
    ACTIONS_DOCUMENTS_SQL,
    ACTIONS_TABLE_SQL,
    DOCUMENTS_TABLE_SQL,
    SUMMARY_TABLE_SQL,
    VOTE_TABLE_SQL,
    MEMBERS_TABLE_SQL,
    ACTIONS_DOCUMENTS_SQL
)

os.makedirs('../db/storage_csvs', exist_ok=True)
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    filename='logs/transform_load.log',
    filemode='w',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
)

def create_tables(json_data:dict, keyword:str):
    file_id = json_data.get('id')
    if keyword=='actions':
        data = json_data.get('actions')
        if not data:
            data = dict(
                date=[],
                activity=[],
                documents=[]
            )
    elif keyword=='documents':
        data = json_data.get('documents')
        if not data:
            data = dict(
                name=[],
                url=[],
                date=[]
            )
    elif keyword == 'summary':
        data = dict(
            url=[json_data.get('url')],
            title=[json_data.get('title')],
            date_received=[json_data.get('date_received')],
            last_modified=[json_data.get('last_modified')],
            expiration=[json_data.get('expiration')],
            reference=[json_data.get('references')],
            district=[json_data.get('district')],
            initiated_by=[json_data.get('initiated_by')],
        )
    elif keyword == 'vote_summary':
        voting_data = json_data['vote_data']
        if voting_data:
            data = dict(
                meeting_date=[voting_data.get('meeting_date')],
                meeting_type=[voting_data.get('meeting_type')],
                vote_action=[voting_data.get('vote_action')],
                vote_given=[voting_data.get('vote_given')]
            )
        else:
            data = dict(
                meeting_date=[],
                meeting_type=[],
                vote_action=[],
                vote_given=[]
            )
    elif keyword == 'vote_members':
        voting_data = json_data['vote_data']
        data = []
        if voting_data:
            data = voting_data.get('members')
        if not data:
            data=dict(
                member_name=[],
                cd=[],
                vote=[]
            )
    else:
        raise Exception('invalid keyword')
    
    frame = pd.DataFrame(data)
    frame['file_id'] = file_id
    return frame

def table_cleaner(json_data:dict):
    f = create_tables
    actions_df = f(json_data, 'actions')
    documents_df = f(json_data, 'documents')
    summary_df = f(json_data, 'summary')
    vote_df = f(json_data, 'vote_summary')
    member_df = f(json_data, 'vote_members')

    actions_df['action_index'] = actions_df.index
    actions_df['action_id'] = actions_df.file_id+'-'\
        +actions_df.action_index.apply(str)+'a'
    documents_df['document_index'] = documents_df.index
    documents_df['document_id'] = documents_df.file_id+'-'\
        +documents_df.document_index.apply(str)+'d'


    actions_documents = actions_df.explode('documents')
    actions_documents['name'] = (
        actions_documents['documents']
        .apply(lambda doc: doc['name'] if not isinstance(doc, float) else pd.NA)
    )
    actions_documents = actions_documents.dropna(subset=['name'])[['name', 'action_id']]
    actions_documents = actions_documents.merge(documents_df, how='left', on='name')
    actions_documents = (
        actions_documents[['action_id', 'document_id']]
        .drop_duplicates(keep='first', ignore_index=True)
    )

    actions_df = actions_df.drop(columns=['action_index', 'documents'])
    documents_df = documents_df.drop(columns=['document_index'])
    return (
        actions_df,
        documents_df,
        summary_df,
        vote_df,
        member_df,
        actions_documents
    )

def parse_ts(raw_date:str):
    return pd.to_datetime(raw_date, format='%Y-%m-%d_%H-%M-%S')

class DataBase:
    def __init__(self):
        self.db_file = "storage.db"
        self.conn = self.create_connection(self.db_file)
        self.execute_query(ACTIONS_TABLE_SQL)
        self.execute_query(DOCUMENTS_TABLE_SQL)
        self.execute_query(SUMMARY_TABLE_SQL)
        self.execute_query(VOTE_TABLE_SQL)
        self.execute_query(MEMBERS_TABLE_SQL)
        self.execute_query(ACTIONS_DOCUMENTS_SQL)
    
    def close_connection(self) -> None:
        if not self.conn.closed:
            self.conn.close()
    
    @staticmethod
    def create_connection(db_file:str):
        """ create a database connection to a SQLite database """
        logging.info(f'Connecting to {repr(db_file)}')
        engine = create_engine(f"sqlite:///../db/{db_file}")
        conn = engine.connect()
        return conn
    
    def execute_query(self, query:str):
        return self.conn.execute(query)
    
    def get_tables(self):
        return self.conn.engine.table_names()
    
    def update_from_frame(self, frame:pd.DataFrame, table:str):
        old_frame = pd.read_sql_table(table, self.conn)
        return (pd
            .concat([old_frame, frame], ignore_index=True)
            .drop_duplicates(keep='first')
            .to_sql(table, self.conn, if_exists='replace', index=False)
        )
    
    def save_db_csvs(self) -> None:
        for table in self.get_tables():
            (pd
              .read_sql_table(table, self.conn)
              .to_csv(f'../db/storage_csvs/{table}.csv', index=False)
            )

if __name__ == '__main__':
    files_by_stamp = {}
    for file_ in os.listdir('../extracted_raw'):
        if file_.endswith('.json'):
            filename, stamp = file_.split('.')[0].split('__')
            files_by_stamp[parse_ts(stamp)] = file_
    newest = files_by_stamp[
        max(files_by_stamp.keys())
    ]
    with open(f'../extracted_raw/{newest}') as f:
        json_ = json.load(f)
    logging.info(f'reading data from {newest}')
    for data in json_:
        logging.info(f'transforming data, file_id = {repr(data["id"])}...')
        frames = table_cleaner(data)
        tables = ('actions', 'documents', 'summary','vote', 'members', 'actions_documents')
        db = DataBase()
        for f, t in zip(frames, tables):
            logging.info(f'saving results to table {repr(t)} and creating')
            try:
                db.update_from_frame(f, t)
            except Exception as e:
                logging.warning(f'error updating from frame, closing connection. Error: {repr(e)}')
                db.conn.close()
                raise e
    logging.info('done, saving csvs representation and closing connection to database')
    db.save_db_csvs()
    db.conn.close()