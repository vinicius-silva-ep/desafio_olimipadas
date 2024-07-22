import pandas as pd

# Exemplo de uso
csv_file_path = 'I:/Meu Drive/ESTUDOS DATA SCIENCE/DESAFIO JOGOS OLÍMPICOS/05-Olimpiadas/olympic_athletes.csv'

# Ler o arquivo CSV com delimitador ','
df = pd.read_csv(csv_file_path, delimiter=',', dtype={'athlete_year_birth': 'Int64'})
df['bio'] = df['bio'].str.replace('\n', ' ', regex=False)
df['athlete_medals'] = df['athlete_medals'].str.replace('\n', ' ', regex=False)

print(df)