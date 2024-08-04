import pandas as pd
import ast
import psycopg2
from psycopg2 import sql

# Nome da tabela para facilitar e renomear em um só lugar
tabela = 'olympic_results'

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
        event_title TEXT,
        slug_game TEXT,
        participant_type TEXT,
        medal_type TEXT,
        rank_equal TEXT,
        rank_position TEXT,
        country_name TEXT,
        country_code TEXT,
        country_3_letter_code TEXT,
        athlete_url TEXT,
        athlete_full_name TEXT,
        value_unit TEXT,
        value_type TEXT,
        athlete_name_1 TEXT,
        athlete_name_2 TEXT
    )
    ''').format(sql.Identifier(schema_name), sql.Identifier(tabela))

cursor.execute(create_table_query)
conn.commit()

# Caminho do arquivo
csv_file_path = 'I:/Meu Drive/ESTUDOS DATA SCIENCE/DESAFIO JOGOS OLÍMPICOS/05-Olimpiadas/olympic_results.csv'

# Ler o arquivo CSV com delimitador ','
df = pd.read_csv(csv_file_path, delimiter=',')

# Função para extrair nomes dos atletas
def extract_athlete_names(athletes_str):
    if pd.isna(athletes_str):
        return []
    try:
        athletes_list = ast.literal_eval(athletes_str)
        return [athlete[0] for athlete in athletes_list]
    except (ValueError, SyntaxError):
        return []
    

# Aplicar a função e expandir as listas em colunas
athlete_names = df['athletes'].apply(extract_athlete_names)
max_athletes = athlete_names.apply(len).max()  # Número máximo de atletas em uma linha


# Adicionar colunas dinamicamente para os nomes dos atletas
for i in range(max_athletes):
    df[f'athlete_name_{i+1}'] = athlete_names.apply(lambda names: names[i] if i < len(names) else None)

# Remover a coluna original 'athletes'
df = df.drop(columns=['athletes'])

# Função para limpar as coluna 'athlete_full_name'
def clean_athlete_name(name):
    if pd.notna(name):
        name = name.lstrip()  # Left trim
        if name.startswith('- ') or name.startswith('. '):
            name = name[2:]  # Remover '- ' se estiver no início
        return name
    return name    

# Aplicar a função de limpeza nas colunas
df['athlete_full_name'] = df['athlete_full_name'].apply(clean_athlete_name)
df['athlete_name_1'] = df['athlete_name_1'].apply(clean_athlete_name)
df['athlete_name_2'] = df['athlete_name_2'].apply(clean_athlete_name)

# Substituir NaN por None em todas as colunas
df = df.where(pd.notnull(df), None)


# Verificar o DataFrame final antes da inserção
print(df.head())

# Preparar os dados para inserção
data_records = df.values.tolist()  # Não substitua NaN por string vazia

# Inserir os dados no banco e faz a verificação se houve algo que deu errado
try:
    # Cria a query SQL parametrizada para inserção em lote
    insert_query = sql.SQL("""
    INSERT INTO {}.{} (discipline_title, event_title, slug_game, participant_type, medal_type, rank_equal, rank_position, country_name, country_code, country_3_letter_code, athlete_url, athlete_full_name, value_unit, value_type, athlete_name_1, athlete_name_2)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(schema_name), sql.Identifier(tabela))

    cursor.executemany(insert_query, data_records)
    conn.commit()
    print("Dados inseridos com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro durante a inserção: {e}")

# Fechar o cursor e a conexão
cursor.close()
conn.close()
