import requests
import pandas as pd
import datetime
import psycopg2
from psycopg2 import Error


def fetch_exchange_rates(base_url, base_currency):
    # Generación de URL para la fecha actual
    url = base_url + base_currency

    # Petición a la API
    response = requests.get(url)

    # Verificación de la respuesta
    if response.status_code == 200:
        # Conversión de la respuesta JSON a un diccionario
        data = response.json()
        return data
    else:
        raise Exception(f"Error al obtener datos de la API: {response.status_code}")


def process_data(data, base_currency, target_currencies):
    # Extracción del tipo de cambio para la moneda base
    base_rate = data["rates"][base_currency] if base_currency in data["rates"] else 1

    # Extracción de los tipos de cambio para las monedas objetivo
    rates = {currency: data["rates"].get(currency, None) for currency in target_currencies}

    # Cálculo de los tipos de cambio con respecto a la moneda base
    converted_rates = {currency: rate / base_rate for currency, rate in rates.items() if rate is not None}

    return converted_rates


def create_dataframe(converted_rates, base_currency):
    # Fecha actual
    today = datetime.date.today()

    # Creación del DataFrame con los datos extraídos
    data = {
        "fecha": [today],
        "moneda_base": [base_currency],
        **converted_rates
    }

    df = pd.DataFrame(data)
    return df


def store_data_in_db(df, db_config):
    conn = None
    try:
        # Conexión a la base de datos
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Creación de la tabla (si no existe)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tipos_cambio (
            fecha DATE,
            moneda_base VARCHAR(3),
            EUR DECIMAL(10,4),
            GBP DECIMAL(10,4),
            JPY DECIMAL(10,4),
            CAD DECIMAL(10,4),
            MXN DECIMAL(10,4)
        )
        """)

        # Inserción de los datos del DataFrame
        for index, row in df.iterrows():
            cursor.execute("""
            INSERT INTO tipos_cambio (fecha, moneda_base, EUR, GBP, JPY, CAD, MXN)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row["fecha"], row["moneda_base"], row.get("EUR"), row.get("GBP"), row.get("JPY"), row.get("CAD"),
                  row.get("MXN")))

        # Guardado de los cambios
        conn.commit()
    except Error as e:
        print(f"Error al conectar o insertar en la base de datos: {e}")
    finally:
        # Cierre de la conexión
        if conn:
            cursor.close()
            conn.close()


# Configuración de la base de datos para Amazon Redshift
db_config = {
    "host": "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    "port": "5439",
    "user": "parela26_coderhouse",
    "password": "FnO723pA7z",
    "dbname": "data-engineer-database"
}

# URL base de la API
base_url = "https://api.exchangerate-api.com/v4/latest/"

# Moneda base
base_currency = "USD"

# Lista de monedas a convertir
target_currencies = ["EUR", "GBP", "JPY", "CAD", "MXN"]

try:
    # Extracción de los datos de la API
    data = fetch_exchange_rates(base_url, base_currency)

    # Procesamiento de los datos
    converted_rates = process_data(data, base_currency, target_currencies)

    # Creación del DataFrame
    df = create_dataframe(converted_rates, base_currency)

    # Visualización del DataFrame (opcional)
    print(df.to_string())

    # Almacenamiento de los datos en la base de datos
    store_data_in_db(df, db_config)

except Exception as e:
    print(f"Error en el proceso ETL: {e}")

