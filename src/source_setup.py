import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

USER = os.getenv("MYSQL_USER")
PASSWORD = os.getenv("MYSQL_PASSWORD")
DB = os.getenv("MYSQL_DATABASE")
HOST = "127.0.0.1" # Asumiendo que lo corres desde tu máquina local apuntando a Docker
PORT = "3307"

def load_grammys_to_mysql():
    print("Conectando a MySQL...")
    engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")
    
    print("Leyendo archivo grammys.csv...")
    # Asegúrate de que el archivo esté en la carpeta data/
    df = pd.read_csv('./data/the_grammy_awards.csv')
    
    print("Insertando datos en la tabla grammy_source...")
    # if_exists='replace' crea la tabla automáticamente si no existe y 'append' inserta
    df.to_sql('grammy_source', con=engine, if_exists='replace', index=False)
    
    # Validar que los datos se subieron
    row_count = pd.read_sql("SELECT COUNT(*) FROM grammy_source", con=engine).iloc[0,0]
    print(f"Carga exitosa. Filas en la base de datos: {row_count}")

if __name__ == "__main__":
    load_grammys_to_mysql()