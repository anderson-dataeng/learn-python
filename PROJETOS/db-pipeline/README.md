# Projeto de Saneamento e Engenharia de Dados

Este projeto tem como objetivo atender uma demanda para saneamento de dados em um processo de engenharia de dados. Utilizamos bibliotecas como `pandas`, `sqlite3` e `dotenv` para realizar as operações necessárias.

## Estrutura do Projeto

- `data_clean(df, metadados)`: Função principal para saneamento dos dados.
- `feat_eng(df, corrige_hr, std_str, tipos_formatted)`: Função para realizar a engenharia de features no DataFrame.
- `save_data_sqlite(df)`: Função para salvar os dados em um banco de dados SQLite.
- `fetch_sqlite_data(table)`: Função para buscar dados do banco de dados SQLite.

## Tecnologias Utilizadas

- Python
- Pandas
- SQLite
- Dotenv
