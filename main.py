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

#-------------------------------FUNCIONES------------------------------#

#---------------------año con mas horas jugadas para dicho género---------------#
@app.get('/PlayTimeGenre')
def PlayTimeGenre(genero: str):
    # Filtrar el DataFrame por el género proporcionado
    genre_df = df_games [df_games ['genres'] == genero]

    # Si ya está agrupado por año y sumadas las horas, no es necesario volver a agrupar.
    # Solo necesitamos encontrar el año con más horas jugadas.
    
    # Encontrar el año con más horas jugadas
    max_year = genre_df.loc[genre_df['play_hours'].idxmax(), 'release_year']

    return {"Año de lanzamiento con más horas jugadas para Género {}: {}".format(genero, max_year)}

    #retorno: {"Año de lanzamiento con más horas jugadas para Género X" : 2013}

#-------------------usuario más horas jugadas para el género por año------------#
@app.get('/UserForGenre')
def UserForGenre(genero : str):
    
    # Filtrar el DataFrame por el género proporcionado
    genre_df = df_games_item[df_games_item['genres'] == genero]
    
    # Encontrar el usuario con más horas jugadas para el género dado
    max_user = genre_df.loc[genre_df['playtime_forever'].idxmax(), 'user_id']

     # Crear una lista de la acumulación de horas jugadas por año
    hours_by_year = genre_df.groupby('release_year')['playtime_forever'].sum().reset_index()
    hours_by_year_list = [{"Año": int(year), "Horas": int(hours)} for year, hours in zip(hours_by_year['release_year'], hours_by_year['playtime_forever'])]

    return {"Usuario con más horas jugadas para Género {}: {}".format(genero, max_user),"Horas jugadas": hours_by_year_list}

    #retorno: {"Usuario con más horas jugadas para Género X" : us213ndjss09sdf, 
              #"Horas jugadas":[{Año: 2013, Horas: 203}, {Año: 2012, Horas: 100}, {Año: 2011, Horas: 23}]} 

#--------------3 de juegos MÁS recomendados por usuarios ----------------------#
@app.get('/UsersRecommend')
def UsersRecommend( año : int):
    # Filtrar el DataFrame df_top3 por el año proporcionado
    top3_by_year = df_games_title[df_games_title['release_year'] == año]
    
    # Crear la lista de diccionarios
    resultado = []
    for index, row in top3_by_year.iterrows():
        puesto = row['rank']
        titulo = row['title']
        año = int(row['release_year'])
        resultado.append({f"Puesto {puesto}": f"{titulo}"})
    return resultado
    # retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]


#------------------- top 3 de desarrolladoras con juegos MENOS recomendados-----------#
@app.get('/UsersWorstDeveloper')
def UsersWorstDeveloper( año : int ):
    # Filtrar el DataFrame df_developer por el año proporcionado
    developer_by_year = df_developers[df_developers['release_year'] == año]

    # Obtener el top 3 de desarrolladoras con juegos MENOS recomendados y sus valores según rank
    top3_worst_developer = developer_by_year.sort_values(by='rank', ascending=True).head(3)

    # Formatear el resultado como lista de diccionarios
    result = [{"Puesto {}: {}".format(rank, developer)} for rank, developer in zip(top3_worst_developer['rank'], top3_worst_developer['developer'])]

    return result


    #retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]

#-------------------------------------------------------------------------#
@app.get('/sentiment_analysis',
         description=""" 
                    INSTRUCCIONES<br>
                    1. Para empezar haga click en -> "Try it out".<br>
                    2. Ingrese el nombre del juego en el recuadro inferior.<br>
                    3. Click a "Execute" ñistar por sentimiento las reseñas
                                      """,
         tags=["Analisis de Sentimiento"])
def sentiment_analysis( empresa_desarrolladora : str): 
    # Filtrar el DataFrame por la empresa desarrolladora proporcionada
    developer_df = df_developers[df_developers['developer'] == empresa_desarrolladora]

    # Obtener la cantidad total de registros para la desarrolladora
    total_records = developer_df['recommend_count'].sum()

    # Crear el diccionario de retorno
    result = {empresa_desarrolladora: {'Negative': 0, 'Neutral': 0, 'Positive': 0}}

    # Llenar el diccionario con la cantidad de registros para cada categoría de sentimiento
    for sentiment, count in zip(developer_df['sentiment_analysis'], developer_df['recommend_count']):
        sentiment_mapping = {0: 'Negative', 1: 'Neutral', 2: 'Positive'}
        sentiment_category = sentiment_mapping[sentiment]
        result[empresa_desarrolladora][sentiment_category] += count

    return result
