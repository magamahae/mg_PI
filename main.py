# Importaciones
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import importlib
import pandas as pd
import operator
from fastapi.responses import JSONResponse

# Se instancia la aplicación
app = FastAPI()

# Carga los datos parquet en un dataframe de pandas
df_reviews_as = pd.read_parquet('DATA/df_developer_sanalysis.parquet')
df_games = pd.read_parquet('DATA/df_genres_year_1.parquet')
df_games_item = pd.read_parquet('DATA/genre_use_year_2.parquet')
df_games_title = pd.read_parquet('DATA/release_year_title_3.parquet')
df_developers = pd.read_parquet('DATA/year_developer_4.parquet')
df_modelo = pd.read_parquet('DATA/modelo_rec.parquet')


#-------------------------------FUNCIONES------------------------------#

#1)---------------------año con mas horas jugadas para dicho género---------------#
@app.get('/PlayTimeGenre')
def PlayTimeGenre(genero: str):
    # Filtrar el DataFrame por el género proporcionado
    genre_df = df_games[df_games ['genres'] == genero]

    # Encontrar el año con más horas jugadas
    max_year = genre_df.loc[genre_df['genres'] == genero, 'release_year'].max()

    #max_year = genre_df.loc[genre_df['play_hours'].idxmax(), 'release_year']
    #return max_year
    return {"Año de lanzamiento con más horas jugadas para Género {}: {}".format(genero, max_year)}

    #retorno: {"Año de lanzamiento con más horas jugadas para Género X" : 2013}
'''

#2)-------------------usuario más horas jugadas para el género por año------------#

@app.get('/UserForGenre')
def UserForGenre(genero):
    #Filtrar por el género especificado
    df_genre = df_games_item[df_games_item['genres'] == genero]

    #Si no hay datos para el género especificado, retorna un mensaje
    if df_genre.empty:
        return f"No hay datos para el género '{genero}'"

    #Agrupar por usuario y género y calcular las horas jugadas sumando los valores
    df_genre_g = df_genre.groupby(['user_id'])['playtime_forever'].sum()

    #Encontrar el usuario con más horas jugadas
    max_playtime = df_genre_g.idxmax()

    #Agrupar por año y calcular las horas jugadas sumando los valores
    grouped_by_year = df_genre.groupby('release_year')['playtime_forever'].sum()

    #Crear lista de acumulación de horas jugadas por año
    horas_acum = [{'Año': year, 'Horas': hours} for year, hours in grouped_by_year.items()]

    #Retornar el resultado como un diccionario
    return {"Usuario con más horas jugadas para Género {}".format(genero): max_playtime, "Horas jugadas": horas_acum}
     

#3)--------------3 de juegos MÁS recomendados por usuarios ----------------------#

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


#4)------------------- top 3 de desarrolladoras con juegos MENOS recomendados-----------#

@app.get('/UsersWorstDeveloper')
def UsersWorstDeveloper( año : int ):
    # Filtrar el DataFrame df_developer por el año proporcionado
    developer_by_year = df_developers[df_developers['release_year'] == año]

    # Obtener el top 3 de desarrolladoras con juegos MENOS recomendados y sus valores según rank
    top3_worst_developer = developer_by_year.sort_values(by='rank', ascending=False).head(3)

    # Formatear el resultado como lista de diccionarios
    result = [{"Puesto {}: {}".format(rank, developer)} for rank, developer in zip(top3_worst_developer['rank'], top3_worst_developer['developer'])]

    return result


    #retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]

#----------------------Analisis de Sentimiento----------------------------------#

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
    developer_df = df_reviews_as[df_reviews_as['developer'] == empresa_desarrolladora]
    
    # Crear el diccionario de retorno
    result = {empresa_desarrolladora: {'Negative': 0, 'Neutral': 0, 'Positive': 0}}

    # Llenar el diccionario con la cantidad de registros para cada categoría de sentimiento
    for sentiment, count in zip(developer_df['sentiment_analysis'], developer_df['recommend_count']):
        sentiment_mapping = {0: 'Negative', 1: 'Neutral', 2: 'Positive'}
        sentiment_category = sentiment_mapping[sentiment]
        result[empresa_desarrolladora][sentiment_category] += count

    return result


#6)------------------------------- item-item-----------------------------------------#

@app.get('/recomendacion_juego',
         description=""" 
                    INSTRUCCIONES<br>
                    1. Para empezar haga click en -> "Try it out".<br>
                    2. Ingrese el nombre del juego en el recuadro inferior.<br>
                    3. Click a "Execute" para juegos recomendados.
                    """,
         tags=["Recomendación"])
def recomendacion_juego(id_producto: int):
    recomendaciones = df_modelo[df_modelo['item_id'] == id_producto]['recomendaciones'].iloc[0]
    
    # Verificar si la lista de recomendaciones no está vacía
    if len(recomendaciones) > 0:
        recomendaciones_dict = {i + 1: juego for i, juego in enumerate(recomendaciones)}
        return recomendaciones_dict
    else:
        # Si no se encontraron recomendaciones para el ID, devolver un mensaje de error
        error_data = {'error': 'No se encontraron recomendaciones para el ID proporcionado'}
        return JSONResponse(content=error_data, status_code=404)

#7)-----------------------------user-item-------------------------------------------#

#@app.get('/recomendacion_juego',)
#def recomendacion_juego(game):
   '''
