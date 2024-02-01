# Visit Coverage -- Looker to Snowflake

import udfs.snowflake_udfs as sf
import udfs.udfs as udf
import udfs.looker_udfs as lk

udf.print_time(text='Started running at:')

run_local=False
query='sql/location_metrics.sql'
if run_local:
    from dotenv import load_dotenv
    load_dotenv()
    pwd_auth=False
    path=query
else:
    pwd_auth=True
    # schema_name='python_scripts'
    path='PalAppReliability/MapQuality/LocationTracking/'+query

# looker query date range (how many days back)
# use udf.output_ranges() if backfilling is needed
rng_start=0
rng_end=rng_start+1

# db and table to write to. Other connection vars are env vars
database_name='ingestion'
schema_name='ingestion_scripts'
table_name='location_metrics'

sql_rfile_frmtd=udf.read_file(path)
sdk = lk.sdk
df_vpn=lk.get_query_loop_range(sql_rfile_frmtd, sdk, rng_start,rng_end)
load_me=df_vpn.drop_duplicates(subset='VISIT_ID', keep='last').reset_index(drop=True)

print("Successfuly queried and imported data to local.")

connection=sf.sf_engine(database=database_name, schema=schema_name, pwd_auth=pwd_auth)
sf.df_to_sf(load_me, table_name, connection, append=True)

print("Successfuly appended to table in Snowflake.")

sql_drop_dups=f"""
    create or replace table {database_name}.{schema_name}.{table_name} as
    select *
    from {database_name}.{schema_name}.{table_name}
    qualify row_number() over (partition by visit_id order by updated_at desc nulls last) = 1
    order by scheduled_for desc, visit_id, updated_at desc
"""
# drop duplicates, keep latest updated_at
ctx=sf.sf_connect()
ctx.cursor().execute(sql_drop_dups)
print("Successfuly removed duplicates from Snowflake")
ctx.close()

udf.print_time(text='Finished running at:')

