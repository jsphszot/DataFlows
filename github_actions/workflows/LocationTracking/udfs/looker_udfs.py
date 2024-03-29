import looker_sdk
import pandas as pd
import udfs.udfs as udf
import datetime as dt

# https://github.com/looker-open-source/sdk-codegen#configuring-lookerini-or-env
# overwrite values in https://github.com/looker-open-source/sdk-codegen/blob/main/looker-sample.ini
# with https://joinpapa.looker.com/admin/users, editing your user, editing API3 keys, and clicking the "reveal" button to view your client_id and client_secret. If there are currently no API3 credentials, they can be generated by clicking “New API3 Key.”
# https://cloud.google.com/looker/docs/api-auth
sdk = looker_sdk.init40()

def run_direct_query(sql_query):
    # def run_direct_query(sql_query, sdk):
    slug=sdk.create_sql_query(body=looker_sdk.models31.SqlQueryCreate( model_name="papa",sql=sql_query)).slug
    result = sdk.run_sql_query(slug=slug, result_format='json', )
    df = pd.read_json(result)
    return df

def read_file_format(path, vardict):
    """
    read file at `path`, fill place holders using `vardict`\n
    `path`: string, path to file.\n
    `vardict`: py dictionary, {'key': value, [...] } pairs
    """
    newline = '\n'  # Avoids SyntaxError: f-string expr cannot include a backslash
    with open(path, 'r') as file:
        myfile = f"{file.read().replace(newline, '')}".format(**vardict)
    return myfile

def get_query_data(sql, sdk):
    slug=sdk.create_sql_query(body=looker_sdk.models31.SqlQueryCreate( model_name="papa",sql=sql)).slug
    result = sdk.run_sql_query(slug=slug, result_format='json',  transport_options={"timeout": int(500)})
    df_vpn=pd.read_json(result)
    return df_vpn

def get_query_loop(sql, sdk, loops):
    """
    play with these loop vals (last 2 args) to get different dates backwards
    """
    df_list=[]
    if loops <= 0:
        return print("`loops` must be greater than 0")
    for num in range(0, loops):
        print(f'running loop {num+1} of {loops}')
        vardict={'dateme': num}
        sql_frmtd=sql.format(**vardict)
        loop_result=get_query_data(sql=sql_frmtd, sdk=sdk)
        df_list.append(loop_result)

    df_vpn=pd.concat(df_list, ignore_index=True)
    df_vpn['UPDATED_AT']=dt.datetime.now()
    df_vpn['UPDATED_AT']=df_vpn['UPDATED_AT'].dt.tz_localize('UTC')
    df_vpn.columns = map(lambda x: str(x).upper(), df_vpn.columns)
    df_vpn = udf.fix_date_cols(df_vpn)
    return df_vpn

def get_query_loop_range(sql, sdk, loop_start, loop_end):
    df_list=[]
    if loop_start >= loop_end:
        return print("`loop_end` must be greater than `loop_start`")
    if loop_end <0 or loop_start <0:
        return print("`loop_start` and `loop_end` cannot be less than 0")
    for num in range(loop_start, loop_end):
        print(f'running loop {num+1} of {loop_end}')
        vardict={'dateme': num}
        sql_frmtd=sql.format(**vardict)
        loop_result=get_query_data(sql=sql_frmtd, sdk=sdk)
        df_list.append(loop_result)

    df_vpn=pd.concat(df_list, ignore_index=True)
    df_vpn['UPDATED_AT']=dt.datetime.now()
    df_vpn['UPDATED_AT']=df_vpn['UPDATED_AT'].dt.tz_localize('UTC')
    df_vpn.columns = map(lambda x: str(x).upper(), df_vpn.columns)
    df_vpn = udf.fix_date_cols(df_vpn)
    return df_vpn

