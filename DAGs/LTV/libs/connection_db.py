import pandas     as pd
import clickhouse_connect
from settings.constants import IP, PORT, DB, USERNAME, PASSWD, table_predict
from libs.label_encoder      import MultiFeatureLabelEncoder

# функция выполнеия SQL запроса
def QueryExecuted(query):
    client           = clickhouse_connect.get_client(host=IP, port=PORT, database=DB, user=USERNAME, password=PASSWD)
    result           = client.query(query)
    df               = pd.DataFrame(
                                        data=result.result_rows,
                                        columns=result.column_names
                                    )
    
    return df

def save_to_clickhouse(total_df):
    try:
        client   = clickhouse_connect.get_client(host=IP, port=PORT, database=DB, user=USERNAME, password=PASSWD)
        client.insert_df(table_predict, total_df)
        return 'Данные успешно сохранены в БД'
        
    except Exception as e:
        return f'Ошибка при вставке данных: {str(e)}'