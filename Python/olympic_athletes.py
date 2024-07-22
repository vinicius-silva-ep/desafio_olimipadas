import pandas as pd
import re
import psycopg2
from psycopg2 import sql

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
    CREATE TABLE IF NOT EXISTS {}.olympic_athletes (
        athlete_url TEXT,
        athlete_full_name TEXT,
        games_participations INT,
        first_game TEXT,
        athlete_year_birth INT,
        athlete_medals TEXT,
        bio TEXT
    )
    ''').format(sql.Identifier(schema_name))

cursor.execute(create_table_query)
conn.commit()

# Caminho do arquivo
csv_file_path = 'I:/Meu Drive/ESTUDOS DATA SCIENCE/DESAFIO JOGOS OLÍMPICOS/05-Olimpiadas/olympic_athletes.csv'

# Ler o arquivo CSV com delimitador ',' e definir o tipo da coluna 'athlete_year_birth' como inteiro
df = pd.read_csv(csv_file_path, delimiter=',')

# Função para limpar o texto das colunas
def clean_text(text):
    if pd.notna(text):
        return re.sub(r'\s+', ' ', text).strip()  # Substituir múltiplos espaços e quebras de linha por um único espaço
    return text

# Aplicar a função nas colunas de interesse
df['bio'] = df['bio'].apply(clean_text)
df['athlete_medals'] = df['athlete_medals'].apply(clean_text)

# Converter colunas para inteiros, com tratamento de valores não numéricos
df['games_participations'] = pd.to_numeric(df['games_participations'], errors='coerce').fillna(0).astype(int)
df['athlete_year_birth'] = pd.to_numeric(df['athlete_year_birth'], errors='coerce').fillna(0).astype(int)

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
    INSERT INTO {}.olympic_athletes (athlete_url, athlete_full_name, games_participations, first_game, athlete_year_birth, athlete_medals, bio)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(schema_name))

    cursor.executemany(insert_query, data_records)
    conn.commit()
    print("Dados inseridos com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro durante a inserção: {e}")

# Fechar o cursor e a conexão
cursor.close()
conn.close()
