import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime

load_dotenv()

def data_clean(df, metadados):
    '''
Função principal para saneamento dos dados
Args:
    df (pd.DataFrame): DataFrame do Pandas contendo os dados brutos.
    metadados (dict): Dicionário de metadados com informações sobre as colunas e tipos de dados.

Returns:
    pd.DataFrame: DataFrame do Pandas com a base de dados tratada.
    '''
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

def feat_eng(df, corrige_hr:list, std_str:list, tipos_formatted:dict):
    """
Função para realizar a engenharia de features no DataFrame.
    - Deleta as colunas com sufixo "_formatted" do DataFrame.
    - Insere os tipos de dados especificados em `tipos_formatted` para as colunas originais do DataFrame.

Args:
    df (pd.DataFrame): DataFrame do Pandas contendo os dados brutos.
    corrige_hr (list): Lista das colunas que tiveram correção de horas.
    std_str (list): Lista das colunas que tiveram padronização de strings.
    tipos_formatted (dict): Dicionário com os tipos de dados formatados para a tabela final.

Returns:
    pd.DataFrame: DataFrame do Pandas com as features de engenharia aplicadas.
    """

    def classifica_hora(hra):
        """
        função para classificar periodo do dia da data do voo para MANHÃ, TARDE, NOITE E MADRUGADA
        """
        if 0 <= hra < 6: return "MADRUGADA"
        elif 6 <= hra < 12: return "MANHA"
        elif 12 <= hra < 18: return "TARDE"
        else: return "NOITE"


    def flg_status(atraso):
        """
        função para criar a coluna com o status do voo
        """
        if atraso > 0.5 : return "ATRASO"
        else: return "ONTIME"

    df_work = df.copy()

    # Consolida as listas de canpos
    formatted_cols_name = std_str + corrige_hr
    #substitui dados das colunas originais pelas colunas com sufixo _formatted
    for col in df_work[formatted_cols_name].columns:
        df_work[col] = df_work[col+'_formatted']
        # deleta coluna com sufixo "_formatted"
        df_work.drop(f'{col}_formatted', axis=1, inplace=True)
    
    # Formata data type das colunas do df
    df_work = utils.convert_data_type(df_work, tipos_formatted)
    

    # tempo_voo_esperado -> datetime_chegada - datetime_partida
    df_work['tempo_voo_esperado'] = (df_work['datetime_chegada'] - df_work['datetime_partida']).dt.total_seconds() / 3600


    # dia_semana -> [data do voo]dt.day_of_week hint: tem que ser do tipo datetime para funcionar
    df_work['dia_semana'] = df_work['data_voo'].dt.day_of_week


    # horario -> classificar em manhã, tarde, noite e madrugada
    df_work["horario"] = df_work.loc[:,"datetime_partida"].dt.hour.apply(lambda x: classifica_hora(x))


    # tempo_voo_hr -> tempo de voo que está em minutos, mas em horas
    df_work["tempo_voo_hr"] = df_work["tempo_voo"] /60

    # atraso -> tempo_voo_hr  - tempo_voo_esperado 
    df_work["atraso"] = df_work["tempo_voo_hr"] - df_work["tempo_voo_esperado"] 

    # flg_status -> classificar quanto tempo de atraso é "ATRASO" e os demais casos "ON-TIME"
    df_work["flg_status"] = df_work.loc[:,"atraso"].apply(lambda x: flg_status(x))


    #colocar log info
    logger.info(
            f"Finalizado Feature de engenharia Dados ; {datetime.datetime.now()}")        
    
    return df_work

def save_data_sqlite(df):
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()


def fetch_sqlite_data(table):
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'),index_col=0)
    df = data_clean(df, metadados)
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves_renamed"])
    df = feat_eng(df, metadados['std_str'], metadados['corrige_hr'], metadados['tipos_formatted'])
    save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    # df.head(500).to_excel('data/amostragem_saida.xlsx', index=False)
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')