
import os
import pandas as pd
from snowflake.connector import connect as snowflake_connect

sf_password=os.getenv('SNOWFLAKE_PASSWORD')
sf_account=os.getenv('SNOWFLAKE_ACCOUNT')
sf_user=os.getenv('SNOWFLAKE_USER')
sf_role=os.getenv('SNOWFLAKE_ROLE')
sf_wh=os.getenv('SNOWFLAKE_WH')


def sf_connect(pwd_auth=True):
    """
    creates Snowflake connection using arg user. \n
    returns a connection object, use as ctx arg in sfsql_df(query, ctx) \n
    `pwd_auth` must be either True (default) or False
    """
    if pwd_auth:
        ctx = snowflake_connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_wh,
            role=sf_role,
        )
    else:
        ctx = snowflake_connect(
            user=sf_user,
            authenticator ='externalbrowser',
            account=sf_account,
            warehouse=sf_wh,
            role=sf_role,
        )
    return ctx

def sfsql_df(query, ctx):
    """
    returns Snowflake query results as pandas dataframe.
    """
    cs = ctx.cursor()
    cs.execute(query)
    df = cs.fetch_pandas_all()
    return df

# to write to SF
# pip install snowflake-sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL 

def sf_engine(database='analytics', schema='reporting', pwd_auth=True):
    """
    requieres Snowflake user account and schema to create a write engine. \n
    returns engine object, use as engine arg in df_to_sf(df, name, engine). \n
    `pwd_auth` must be either True (default) or False
    """
    if pwd_auth:
        engine = create_engine(
            URL(
                user=sf_user,
                password=sf_password,
                account=sf_account,
                database=database,
                schema=schema,
                warehouse=sf_wh,
                role=sf_role,
            )
        )
    else:
        engine = create_engine(
            URL(
                user=sf_user,
                authenticator ='externalbrowser',
                account=sf_account,
                database=database,
                schema=schema,
                warehouse=sf_wh,
                role=sf_role,
            )
        )
    return engine

def sf_pwd_engine(database='analytics', schema='reporting'):
    """
    requieres Snowflake user account and schema to create a write engine.
    returns engine object, use as engine arg in df_to_sf(df, name, engine).
    """
    engine = create_engine(URL(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        database=database,
        schema=schema,
        warehouse=sf_wh,
        role=sf_role,
    ))
    return engine

# from snowflake.connector.pandas_tools import write_pandas 
from snowflake.connector.pandas_tools import pd_writer
def df_to_sf(df, name, engine, append=True):
    """
    creates or overwrites table 'name' from a pandas dataframe to database.schema in snowflake, using connection created in provided engine.
    """
    if append:
        method='append'
    else:
        method='replace'
    df.to_sql(
        name=name.lower(), 
        con=engine, 
        index=False, 
        method=pd_writer, 
        if_exists=method #'replace', # 'append'
    )
    
