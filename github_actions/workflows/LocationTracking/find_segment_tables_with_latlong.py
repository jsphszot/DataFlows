import udfs.snowflake_udfs as sf
import udfs.udfs as udf
import udfs.looker_udfs as lk

connection=sf.sf_engine(user='jszot@joinpapa.com', schema='dev_josephs')
sdk = lk.sdk

latlon_seg_cols_query="""
-- show tables with columns like lon% or lat%
select 
-- distinct table_name, table_schema, table_catalog
distinct table_name
-- *
from segment_events.information_schema.columns 
where table_schema = 'PAPA_PAL_PROD' 
and column_name ilike any ('lon%', 'lat%') 
order by TABLE_NAME
"""

ctx=sf.sf_connect(user='jszot@joinpapa.com')
latlon_tables=sf.sfsql_df(query=latlon_seg_cols_query, ctx=ctx)
ctx.close()


import pandas as pd
def segment_latlon_looper():
    ctx_fx=sf.sf_connect(user='jszot@joinpapa.com')
    empty_list=[]
    for x in latlon_tables.loc[:, 'TABLE_NAME']:
        # print(x)
        loop_query=f"""
            select 
            -- longitude, latitude, sent_at,
            -- *
            '{x}' as table_name
            ,count(*) as non_null_latlon_rows
            ,max(sent_at), max(timestamp)
            ,min(sent_at), min(timestamp)
            ,avg(latitude), avg(longitude)
            from segment_events.papa_pal_prod.{x}
            where longitude is not null and latitude is not null
        """
        # print(loop_query)
        loop_df=sf.sfsql_df(query=loop_query, ctx=ctx_fx)
        empty_list.append(loop_df)
    ctx_fx.close()
    return pd.concat(empty_list)

df=segment_latlon_looper()

df.to_csv('lat_lon_in_segment.csv')
