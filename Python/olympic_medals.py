import pandas as pd
import re
import psycopg2
from psycopg2 import sql

# Nome da tabela para facilitar e renomear em um só lugar
tabela = 'olympic_medals'

# Configurações de conexão
host = 'localhost'
dbname = 'DW_IMP_EXP'
user = 'postgres'
password = 'Estudos123'
port = '5432'
schema_name = 'public'

# Conectar ao banco de dados PostgreSQL
conn = psycopg2.connect(
    host=host,
    dbname=dbname,
    user=user,
    password=password,
    port=port,
    options=f'-c search_path={schema_name}'
)

# Criar um cursor
cursor = conn.cursor()

# Criar a tabela com chave primária composta
create_table_query = sql.SQL('''
    CREATE TABLE IF NOT EXISTS {}.{} (
        discipline_title TEXT,
        slug_game TEXT,
        event_title TEXT,
        event_gender TEXT,
        medal_type TEXT,
        participant_type TEXT,
        participant_title TEXT,                             
        athlete_url TEXT,   
        athlete_full_name TEXT,
        country_name TEXT,
        country_code TEXT,      
        country_3_letter_code TEXT                    
    )
    ''').format(sql.Identifier(schema_name), sql.Identifier(tabela))

cursor.execute(create_table_query)
conn.commit()

# Caminho do arquivo
csv_file_path = 'I:/Meu Drive/ESTUDOS DATA SCIENCE/DESAFIO JOGOS OLÍMPICOS/05-Olimpiadas/olympic_medals.csv'

# Ler o arquivo CSV com delimitador ','
df = pd.read_csv(csv_file_path, delimiter=',')

# Substituir NaN por None em todas as colunas
df = df.where(pd.notnull(df), None)

# Exibir os primeiros registros para confirmar o carregamento
print(df.head())

# Preparar os dados para inserção
data_records = df.values.tolist()

# Inserir os dados no banco e faz a verificação se houve algo que deu errado
try:
    # Cria a query SQL parametrizada para inserção em lote
    insert_query = sql.SQL("""
    INSERT INTO {}.{} (discipline_title, slug_game, event_title, event_gender, medal_type, participant_type, participant_title, athlete_url, athlete_full_name, country_name, country_code, country_3_letter_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(schema_name), sql.Identifier(tabela))

    cursor.executemany(insert_query, data_records)
    conn.commit()
    print("Dados inseridos com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro durante a inserção: {e}")

# Fechar o cursor e a conexão
cursor.close()
conn.close()
