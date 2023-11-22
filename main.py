from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import pandas as pd

app = FastAPI()

#http://127.0.0.1:8000 (ruta raiz)
@app.get("/")                       #ruta
def read_root():                    #FUNCION EN ESTA RUTA
    return {"Hello": "World"}

# Carga los datos parquet en un dataframe de pandas
df_reviews_as = pd.read_parquet('DATA/df_developer_sanalysis.parquet')
df_games = pd.read_parquet('DATA/df_genres_year_1.parquet')
df_games_item = pd.read_parquet('DATA/genre_use_year_2.parquet')
df_games_title = pd.read_parquet('DATA/release_year_title_3.parquet')
df_developers = pd.read_parquet('DATA/year_developer_4.parquet')

#---------------------año con mas horas jugadas para dicho género---------------#
@app.get('/PlayTimeGenre')
def PlayTimeGenre(genero: str):
    # Filtrar el DataFrame por el género proporcionado
    genre_df = df_games [df_games ['genres'] == genero]

    # Encontrar el año con más horas jugadas
    max_year = genre_df.loc[genre_df['genres'] == genero, 'release_year'].max()

    #max_year = genre_df.loc[genre_df['play_hours'].idxmax(), 'release_year']
    #return max_year
    return {"Año de lanzamiento con más horas jugadas para Género {}: {}".format(genero, max_year)}

    #retorno: {"Año de lanzamiento con más horas jugadas para Género X" : 2013}
