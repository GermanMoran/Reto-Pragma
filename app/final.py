# =======================================================
# Librerias y Dependencias
# =======================================================
import os
import csv
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from dateutil import parser as dateparser
from decimal import Decimal, InvalidOperation
from datetime import datetime
import argparse


# =======================================================
# Configuración Global
# =======================================================

'''
    Se define la carpeta raiz de los datos 
'''
CSV_DIR = "./datos"
SOURCE_PREFIX = "2012-"  
STATS_NAME = "global"

# Configuracion Inicial de la base de datos
# -- Caso de prueba localhost 
# --- Docker : cambiar localhost a nombre del servido (postgres_db) dentro archivo .env
DB_CONF = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "db_batch"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "user"),
}


# =======================================================
# SQL Queries
# =======================================================

# Inicializar estadisticas 
INIT_STATS_ROW_SQL = """
INSERT INTO BT.stats(name, cnt, ssum, smin, smax)
VALUES('global', 0, 0.0, NULL, NULL)
ON CONFLICT (name) DO NOTHING;
"""

#Insertar registros dentro de la tabla transaciones
INSERT_TX_SQL = """
INSERT INTO BT.transactions (timestamp, price, user_id, source_file)
VALUES %s
ON CONFLICT DO NOTHING;
"""

#Actualizar estadisticas
UPDATE_STATS_SQL = """
UPDATE BT.stats
SET
  cnt = cnt + %s,
  ssum = ssum + %s,
  smin = CASE WHEN smin IS NULL THEN %s ELSE LEAST(smin, %s) END,
  smax = CASE WHEN smax IS NULL THEN %s ELSE GREATEST(smax, %s) END
WHERE name = %s;
"""

# Obtener estadisticas desde la Tabla SQL
GET_STATS_SQL = "SELECT cnt, ssum, smin, smax FROM BT.stats WHERE name = %s;"

# Verificar si el archivo .csv ya esta ingestado
CHECK_FILE_SQL = "SELECT 1 FROM BT.ingestion_log WHERE file_name = %s;"

# Insertar log del archivo ingestado dentro de la tabla ingestion_log
LOG_FILE_SQL = """
INSERT INTO BT.ingestion_log(file_name, rows_loaded, loaded_at)
VALUES (%s, %s, %s)
ON CONFLICT (file_name)
DO UPDATE SET rows_loaded = EXCLUDED.rows_loaded,
              loaded_at   = EXCLUDED.loaded_at;
"""


# Queries para crear tablas
DDL = {
    "transactions": """
        CREATE TABLE IF NOT EXISTS BT.transactions (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            user_id INTEGER NOT NULL,
            source_file TEXT NOT NULL
        );
    """,
    "ingestion_log": """
        CREATE TABLE IF NOT EXISTS BT.ingestion_log (
            file_name   TEXT PRIMARY KEY,
            rows_loaded INTEGER NOT NULL,
            loaded_at   TIMESTAMP NOT NULL
        );
    """,
    "stats": """
        CREATE TABLE IF NOT EXISTS BT.stats (
            name TEXT PRIMARY KEY CHECK (name = 'global'),
            cnt  BIGINT NOT NULL,
            ssum DOUBLE PRECISION,
            smin DOUBLE PRECISION,
            smax DOUBLE PRECISION
        );
    """,
}


# =======================================================
# Funciones auxiliares
# =======================================================

"""
Esta funcioón permite crear tablas necesarias dentro de postgres 
e inicializa fila dentr 1 de la tabla estadísticas.
"""

def init_db(conn):
    with conn.cursor() as cur:
         #Crear el esquema primero
        cur.execute("CREATE SCHEMA IF NOT EXISTS BT;")
        for sql in DDL.values():
            cur.execute(sql)
        cur.execute(INIT_STATS_ROW_SQL)
    conn.commit()



'''
Esta función permite truncar las tablas 
'''
def truncate_tables(conn):

    cur = conn.cursor()
    try:
        print("[INFO] Limpiando tablas...")
        cur.execute("TRUNCATE TABLE BT.transactions RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE BT.ingestion_log ")
        cur.execute("TRUNCATE TABLE BT.stats;")

        conn.commit()
        print("[OK] Tablas reseteadas correctamente")
    except Exception as e:
        print("[ERROR] No se pudo truncar tablas:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        


""" Esta función permite buscar archivos de origen 2012-*.csv
      (excluyendo validation)."""
      
def find_source_files(directory):
    files = [f for f in os.listdir(directory) if f.endswith(".csv")]
    files = [f for f in files if f.lower() != "validation.csv"]
    files = [f for f in files if f.startswith(SOURCE_PREFIX)]
    files.sort(key=lambda x: [int(p) if p.isdigit() else p for p in x.replace(".csv", "").split("-")])
    return files



"""
Esta función permite convertir cada  fila CSV en (timestamp, price, user_id)
en el formato de datos correcto para realizar el cargue en la base de datos
"""
def parse_row(row):

    ts_raw = row.get("timestamp")          # Columna timestamp
    price_raw = row.get("price")           # Columna price
    user_id = row.get("user_id")           # Columna user_id
    
    # Verificacion columa price no sea vacia
    if not price_raw or price_raw.strip() == "":
        raise ValueError("price empty")

    try:
        price = Decimal(price_raw.replace(",", "").strip())
    except InvalidOperation:
        raise ValueError(f"price invalid: {price_raw}")

    # Convierte la cadena a formato datetime - garantizando esquema BD
    ts = dateparser.parse(ts_raw) if ts_raw else None
    # Conversión de la columna user_id a entero
    user_id = int(user_id) if user_id is not None else None
    return ts, price, user_id



"""
Esta función permite:
- Procesar cada uno de los archivos(CSV) en microbatches
- permite actualziar las estadisticas dentro de la tabla  stats

Entradas:
    - Tamaño del Lote (Batch): defecto (100)  se peude ajustar (1, cada fila es un lote)

"""

def process_file(conn, filepath, source_file_name, microbatch_size=100):
    
    # Crear cursor para enviar y recibir ordenes SQL
    cur = conn.cursor()

    # Verificar si el archivo CSV  ya fue procesado
    cur.execute(CHECK_FILE_SQL, (source_file_name,))
    if cur.fetchone():
        print(f"[SKIP] {source_file_name} ya procesado previamente.")
        cur.close()
        return 0


    inserted_total = 0
    batch, batch_prices = [], []

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Lectura de cada fila para cada archivo 
        for idx, row in enumerate(reader, start=1):
            try:
                # Conversion al tipo de dato requerido
                ts, price, user_id = parse_row(row)
            except Exception as e:
                print(f"[WARN] Fila {idx} inválida en {source_file_name}: {e}")
                continue
            
            # Se agrega c/d fila en formato de tupla (4) a la lista batch
            batch.append((ts, price, user_id, source_file_name))
            # Dentro de la lista batch_prices se agrega el precio de c/d registro para posteriores calculos.
            batch_prices.append(price)
            
            # Se garantiza que se ejecute el proceso de acuerdo al tamaño de cada batch, no todo el archivo a la vez
            if len(batch) >= microbatch_size:
                flush_batch(conn, cur, batch, batch_prices)
                inserted_total += len(batch)
                batch, batch_prices = [], []

    # procesar sobrantes
    if batch:
        flush_batch(conn, cur, batch, batch_prices)
        inserted_total += len(batch)

    # Registrar archivo
    cur.execute(LOG_FILE_SQL, (source_file_name, inserted_total, datetime.now()))
    conn.commit()
    cur.close()

    print(f"[OK] {source_file_name} -> {inserted_total} filas insertadas.")
    return inserted_total


"""
Esta función permite insertar un  microbatch dentro de la tabla transactions y actualizar la tabla  stats.
"""

def flush_batch(conn, cur, batch, prices):
    # Se inserta la fila dentro de la tabla transactions , deacuerdo a los valores obtenidos
    execute_values(cur, INSERT_TX_SQL, batch, template="(%s, %s, %s, %s)")
    # Se actulizan las estdiasticas de la tabla stats deaceurdo a los datos del batch ingestado
    cur.execute(
        UPDATE_STATS_SQL,
        (len(batch), sum(prices), min(prices), min(prices), max(prices), max(prices), STATS_NAME),
    )
    conn.commit()
    print(f"[INFO] Batch de {len(batch)} filas insertado.....")



"""
Esta funcion permite mostrar las estadisticas acumuladas
"""
def print_stats(conn, label=""):
    cur = conn.cursor()
    # Se recuperan las estadisticas acumuladas que se han ido insertado ejecución tras ejeución
    # Se realiza la consula directamente a la tabla estadisticas (stats), no se obtienen las 
    # estadisticas directamente de la tabla transactions
    cur.execute(GET_STATS_SQL, (STATS_NAME,))
    row = cur.fetchone()
    cur.close()

    if row:
        cnt, ssum, smin, smax = row
        # Se obtienne el promedio del precio como:  suma del precio del batch / numero de registros del batch
        mean = (ssum / cnt) if cnt else None
        print(f"{label} STATS -> rows={cnt}, sum={ssum}, min={smin}, max={smax}, mean={mean}")
    else:
        print("No hay estadísticas registradas.")


# =======================================================
# Main CLI
# =======================================================
def main():
    parser = argparse.ArgumentParser(description="Pipeline de ingesta de datos en microbatch")
    parser.add_argument("--chunksize", type=int, default=100, help="Número de filas por microbatch")
    parser.add_argument("--validation", action="store_true", help="Procesar también validation.csv")
    parser.add_argument("--truncate-tables", action="store_true",
                        help="Truncar todas las tablas antes del proceso")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONF)
    init_db(conn)

    try:
        if args.truncate_tables:
            truncate_tables(conn)
            return 
        
        # caso: correr solo archivo de validación
        if args.validation:
            val_path = os.path.join(CSV_DIR, "validation.csv")
            if os.path.exists(val_path):
                print(f"\n=== Procesando SOLO validation.csv ===")
                process_file(conn, val_path, "validation.csv", microbatch_size=args.chunksize)
                print("\n>>> Estadísticas tras validation.csv")
                print_stats(conn)
            else:
                print("[WARN] No se encontró validation.csv")
            return
        
        # 2. Procesar archivos 2012-*.csv
        files = find_source_files(CSV_DIR)
        for fname in files:
            print(f"\n=== Procesando {fname} ===")
            process_file(conn, os.path.join(CSV_DIR, fname), fname, microbatch_size=args.chunksize)
            print_stats(conn, label=f"[Después de {fname}]")

        # 2. Mostrar acumulado
        print("\n>>> Estadísticas acumuladas tras 2012-*.csv")
        print_stats(conn)


    finally:
        conn.close()


if __name__ == "__main__":
    main()
