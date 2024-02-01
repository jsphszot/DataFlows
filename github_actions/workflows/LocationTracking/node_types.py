# visit nodes activity type

import udfs.snowflake_udfs as sf
import udfs.udfs as udf
import udfs.looker_udfs as lk

udf.print_time(text='Started running at:')

run_local=False
query='sql/node_types.sql'
if run_local:
    from dotenv import load_dotenv
    load_dotenv()
    pwd_auth=False
    path=query
else:
    pwd_auth=True
    # schema_name='python_scripts'
    path='PalAppReliability/MapQuality/LocationTracking/'+query

loops_to_loop=2

rng_start=0
rng_end=rng_start+loops_to_loop

database_name='ingestion'
schema_name='ingestion_scripts'
table_name='visit_node_activity_types'

sql_rfile_frmtd =udf.read_file(path)
sdk = lk.sdk
df_vpn=lk.get_query_loop_range(sql_rfile_frmtd, sdk, rng_start,rng_end)

unique_key=['VISIT_ID', 'PAL_ID', 'ACTIVITY_TYPE', 'PART_OF_VISIT']
load_me=df_vpn.drop_duplicates(subset=unique_key, keep='last').reset_index(drop=True)
print("Successfuly queried and imported data to local.")

connection=sf.sf_engine(database=database_name, schema=schema_name, pwd_auth=pwd_auth)

load_me.loc[~load_me['DL_NUMBER_FOUNTAIN'].isna(), 'DL_NUMBER_FOUNTAIN']='Has DL'

sf.df_to_sf(load_me, table_name, connection, append=True)
print("Successfuly appended to table in Snowflake.")

sql_drop_dups=f"""
    create or replace table {database_name}.{schema_name}.{table_name} as
    select *
    from {database_name}.{schema_name}.{table_name}
    qualify row_number() over (partition by {', '.join(unique_key).lower()} order by updated_at desc nulls last) = 1
"""

ctx=sf.sf_connect(pwd_auth=pwd_auth)
ctx.cursor().execute(sql_drop_dups)
print("Successfuly removed duplicates from Snowflake")
ctx.close()

udf.print_time(text='Finished running at:')
