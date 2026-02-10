
import supabase
import pandas as pd

def read_table_from_supabase(table_name, client):
    "reads table from supabase ad returns dataframe"
    response = client.from_(table_name).select("*").execute()
    df = pd.DataFrame(response.data)
    return df

def get_column_names_from_supabase(table_name, client):
    "reads table and gets column names"
    
    df = read_table_from_supabase(table_name, client)
    
    return list(df)

def read_schedule_from_supabase(season, week, client):
    """reads schedule from supabase db"""
    
    table_name = 'Schedule'
    
    # convert to data frame
    schedule = read_table_from_supabase(table_name, client)
    
    # only return the week we were looking for
    schedule = schedule[
        (schedule['Week'] == week)
        &
        (schedule['Season'] == season)
    ]
    
    return schedule

def read_selected_columns_by_year(table_name, columns, season, client):
    
    """
    Queries supabase for a given year. uses the Scheule table to query the year
    
    table_name: str
    columns: list
    season: float or int
    client: supabase client
    
    
    """
    
    # convert list of columns to string
    columns_str = ", ".join(columns)
    query_str = "key, " + columns_str + ", Schedule(Season)"
    
    # print(query_str)
    
    response = (
        client
        .table(table_name)
        .select(query_str)
        .eq('Schedule.Season', season)
        .execute()
        )

    # Get the filtered data
    filtered_data = response.data
    # print(filtered_data)

    range_list = []

    for i in range(0,len(filtered_data)):
        if filtered_data[i]['Schedule'] != None:
            # print(filtered_data[i])
            range_list.append(filtered_data[i])

    df = pd.DataFrame(range_list)
    
    #df = df.drop("Schedule", axis=1)
    # print(df.shape)
    return df 
    
    