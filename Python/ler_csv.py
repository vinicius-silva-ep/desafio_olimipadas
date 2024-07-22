# Esse arquivo serve para ver como está o andamento do csv e verificar as colunas

import pandas as pd

# Exemplo de uso
csv_file_path = 'I:/Meu Drive/ESTUDOS DATA SCIENCE/DESAFIO JOGOS OLÍMPICOS/05-Olimpiadas/olympic_medals.csv'

# Ler o arquivo CSV com delimitador ','
df = pd.read_csv(csv_file_path, delimiter=',')

print(df)