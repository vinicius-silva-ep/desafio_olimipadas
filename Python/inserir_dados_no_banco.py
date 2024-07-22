import pandas as pd
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
        games_participations TEXT,
        first_game TEXT,
        athlete_year_birth TEXT,
        athlete_medals TEXT,
        bio TEXT
    )
    ''').format(sql.Identifier(schema_name))

cursor.execute(create_table_query)
conn.commit()

# Função para limpar e padronizar a coluna athlete_medals
def clean_medals(medals):
    if pd.isna(medals):
        return medals
    medals = medals.replace("\n", "").replace(" ", "")
    return medals

# Exemplo de uso
csv_file_path = 'I:/Meu Drive/ESTUDOS DATA SCIENCE/DESAFIO JOGOS OLÍMPICOS/05-Olimpiadas/olympic_athletes.csv'

# Ler o arquivo CSV
df = pd.read_csv(csv_file_path)

# Exibir os primeiros registros para confirmar o carregamento
print(df.head())

# Remover espaços em branco dos nomes das colunas
df.columns = df.columns.str.strip()

# Aplicar a função de limpeza na coluna athlete_medals
df['athlete_medals'] = df['athlete_medals'].apply(clean_medals)

# Preparar os dados para inserção
data_records = df.values.tolist()

# Inserir os dados no banco e faz a verificação se houve algo que deu errado
try:
    # Cria a query SQL parametrizada para inserção em lote
    insert_query = sql.SQL("""
    INSERT INTO {}.olympic_athletes (athlete_url, athlete_full_name, games_participations, first_game, athlete_year_birth, athlete_medals, bio )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(schema_name))
    cursor.executemany(insert_query, data_records)
    conn.commit()
    print("Dados inseridos com sucesso!") # Aguardar a mensagem!
except Exception as e:
    print(f"Ocorreu um erro durante a inserção: {e}")

# Fechar o cursor e a conexão
cursor.close()
conn.close()
