import pandas as pd
import sqlalchemy

import config.settings as settings

def revisar_ultima_actualizacion(cliente_id: str):
    # establece la conexión a la base de datos
    engine = sqlalchemy.create_engine(settings.POSTGRES_URI)
    
    # consulta para obtener la fecha de la última actualización
    qry=f"""
    SELECT fecha_registro
    FROM {settings.TABLA_REGISTRO_CASOS}
    WHERE cliente = '{cliente_id}'
    ORDER BY fecha_registro DESC
    LIMIT 1
    """
    df = pd.read_sql_query(qry, engine)
    
    # obtiene los datos de la última actualización
    fecha_registro = df['fecha_registro'].values[0]
    qry=f"""
    SELECT *
    FROM {settings.TABLA_REGISTRO_CASOS}
    WHERE cliente = '{cliente_id}'
    AND fecha_registro = '{fecha_registro}'
    """
    df = pd.read_sql_query(qry, engine)
    
    return df

def guardar_en_db(datos: pd.DataFrame, tabla: str):
    # establece la conexión a la base de datos
    engine = sqlalchemy.create_engine(settings.POSTGRES_URI)

    # guarda los datos en la base de datos
    datos.to_sql(tabla, engine, if_exists='append', index=False)