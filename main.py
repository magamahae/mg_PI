from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

class Libro(BaseModel):
    titulo: str
    autor: str
    paginas: int
    editorial: str

#http://127.0.0.1:8000 (ruta raiz)
@app.get("/")                       #ruta
def read_root():                    #FUNCION EN ESTA RUTA
    return {"Hello": "World"}

@app.get("/Libros/{id}")
def mostrar_libro(id: int):
    return {"data":id}

@app.post("/Libros")
def insertar_libro(libro: Libro):
    return {"message": f"libro {libro.titulo} insertado"}