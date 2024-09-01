import pandas as pd
import re
import logging
import datetime


logging.basicConfig(filename='data/flights_pipe_log.log', level=logging.INFO)
logger = logging.getLogger()

def read_metadado(meta_path):
    '''
Função para ler metadados de um arquivo Excel e organizar as informações em um dicionário.
INPUT:\n * <b>meta_path</b> (str): Caminho para o arquivo Excel contendo os metadados.
OUTPUT:
    * dict: Dicionário contendo várias informações extraídas dos metadados, incluindo:
        - tabela (numpy.ndarray): Valores únicos da coluna 'tabela'.
        - cols_originais (list): Lista das colunas originais.
        - cols_renamed (list): Lista das colunas renomeadas.
        - tipos_originais (dict): Dicionário mapeando colunas originais para seus tipos originais.
        - tipos_formatted (dict): Dicionário mapeando colunas renomeadas para seus tipos formatados.
        - cols_chaves (list): Lista das colunas chaves originais.
        - cols_chaves_renamed (list): Lista das colunas chaves renomeadas.
        - null_tolerance (dict): Dicionário mapeando colunas renomeadas para sua tolerância a nulos.
        - std_str (list): Lista das colunas renomeadas que precisam de padronização de strings.
        - corrige_hr (list): Lista das colunas renomeadas que precisam de correção de horário.

    '''
    meta = pd.read_excel(meta_path)
    metadados = {
        "tabela": meta["tabela"].unique(),
        "cols_originais" : list(meta["cols_originais"]), 
        "cols_renamed" : list(meta["cols_renamed"]),
        "tipos_originais" : dict(zip(list(meta["cols_originais"]),list(meta["tipo_original"]))),
        "tipos_formatted" : dict(zip(list(meta["cols_renamed"]),list(meta["tipo_formatted"]))),
        "cols_chaves" : list(meta.loc[meta["key"] == 1]["cols_originais"]),
        "cols_chaves_renamed" : list(meta.loc[meta["key"] == 1]["cols_renamed"]),
        "null_tolerance" : dict(zip(list(meta["cols_renamed"]), list(meta["raw_null_tolerance"]))),
        "std_str" : list(meta.loc[meta["std_str"] == 1]["cols_renamed"]),
        "corrige_hr" : list(meta.loc[meta["corrige_hr"] == 1]["cols_renamed"])
        }
    return metadados

# Funções de Saneamento ----------------------------------------------------------------

def null_exclude(df, cols_chaves):
    """
Função de exclusão das observações nulas.

Args:
    df (pd.DataFrame): DataFrame do Pandas.
    cols_chaves (list): Lista de colunas que são chaves.

Returns:
    pd.DataFrame: DataFrame do Pandas com as observações nulas excluídas.
    """
    tmp = df.copy()
    for col in cols_chaves:
        tmp_df = tmp.loc[~df[col].isna()]
        tmp = tmp_df.copy()
    return tmp_df

def select_rename(df, cols_originais, cols_renamed):
    """
Função para renomear colunas.

Args:
    df (pd.DataFrame): DataFrame do Pandas.
    cols_originais (list): Lista dos nomes das colunas originais.
    cols_renamed (list): Lista dos novos nomes das colunas.

Returns:
    pd.DataFrame: DataFrame do Pandas com os novos nomes das colunas.
    """
    df_work = df.loc[:, cols_originais].copy()
    columns_map = dict(zip(cols_originais,cols_renamed))
    df_work.rename(columns=columns_map, inplace = True)
    return df_work

def convert_data_type(df, tipos_map):
    """
Função que converte o tipo do dado.

Args:
    df (pd.DataFrame): DataFrame do Pandas.
    tipos_map (dict): Dicionário com colunas como chave e seus tipos como valores.

Returns:
    pd.DataFrame: DataFrame do Pandas com os tipos de dados convertidos.
    """
    data = df.copy()
    for col in tipos_map.keys():
        tipo = tipos_map[col]
        if tipo == "int":
            tipo = data[col].astype(int)
        elif tipo == "float":
            data[col] = data[col].astype(float)
        elif tipo == "datetime":
            data[col] = pd.to_datetime(data[col])
        elif tipo == "string":
            data[col] = data[col].astype(str)
    return data


def string_std(df, std_str):
    """
Função para criar colunas padronizadas.

Adiciona novas colunas com o sufixo _formatted no retorno.

Args:
    df (pd.DataFrame): DataFrame do Pandas.
    std_str (list): Lista das colunas que devem receber a padronização de strings.

Returns:
    pd.DataFrame: DataFrame do Pandas com as colunas padronizadas.
    """
    df_work = df.copy()
    for col in std_str:
        new_col = f'{col}_formatted'
        df_work[new_col] = df_work.loc[:,col].apply(lambda x: padroniza_str(x))
    return df_work

# Funções de validação
def null_check(df, null_tolerance):
    """
Função de validação de nulos.

Args:
    df (pd.DataFrame): DataFrame do Pandas.
    null_tolerance (dict): Dicionário de colunas como chave e critério de nulo como valores.

Returns:
    pd.DataFrame: DataFrame do Pandas.
    """
    for col in null_tolerance.keys():
        if  len(df.loc[df[col].isnull()])/len(df)> null_tolerance[col]:
            logger.error(
                f"{col} possui mais nulos do que o esperado; {datetime.datetime.now()}")
        else:
             logger.info(
                f"{col} possui nulos dentro do esperado; {datetime.datetime.now()}")

def keys_check(df, cols_chaves:list):
    """
Função para validar se a quantidade de linhas das colunas chaves sem duplicidade é igual ao tamanho do mesmo DataFrame.

Args:
    df (pd.DataFrame): DataFrame do Pandas.
    cols_chaves (list): Lista com as colunas chaves.

Returns:
    None: Mensagem de aviso no log.
    """
    if len(df[cols_chaves].drop_duplicates()) == len(df):
        pass
    else:
        #colocar log info
        logger.info(
            f"[{len(df.loc[df[cols_chaves].duplicated()])}] Dados duplicados ; {datetime.datetime.now()}")        
    pass

# Funções auxiliares -------------------------------------------

def padroniza_str(obs):
    return re.sub('[^A-Za-z0-9]+', '', obs.upper())


def corrige_hora(hr_str, dct_hora = {1:"000?",2:"00?",3:"0?",4:"?"}):
    if hr_str == "2400":
        return "00:00"
    elif (len(hr_str) == 2) & (int(hr_str) <= 12):
        return f"0{hr_str[0]}:{hr_str[1]}0"
    else:
        hora = dct_hora[len(hr_str)].replace("?", hr_str)
        return f"{hora[:2]}:{hora[2:]}"