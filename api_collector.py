import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# ======================================
# CONFIGURACIÓN (SEGURA CON VARIABLES DE ENTORNO)
# ======================================
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}
WAREHOUSE_CODES = os.getenv("WAREHOUSE_CODES", "1145,1290").split(",")

# Configuración de comportamiento
MAX_RETRIES = 2
REQUEST_DELAY = 30
RETRY_DELAY = 10

# ======================================
# DEFINICIÓN DE ENDPOINTS Y CONFIGURACIÓN DE CONSULTAS
# ======================================
ENDPOINT = "/System.Transactions.List.View1"  # Endpoint único para transacciones

QUERY_CONFIG = [
    {
        "name": "small_sales",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '313%%' and (ctxn_primary_qty > 0) and (ctxn_primary_qty <= 3)"
        }
    },
    {
        "name": "medium_sales",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '313%%' and (ctxn_primary_qty > 3) and (ctxn_primary_qty <= 50)"
        }
    },
    {
        "name": "large_sales",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '313%%' and (ctxn_primary_qty > 50)"
        }
    },
    {
        "name": "other_movements",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "20000",
            "where": "(ctxn_movement_type not ilike '313%%') and (ctxn_movement_type not ilike '311%%') and (ctxn_movement_type not ilike '261%%') and (ctxn_movement_type not ilike '344%%') and (ctxn_movement_type not ilike '327%%') and (ctxn_movement_type not ilike '349%%') and (ctxn_movement_type not ilike '325%%') and (ctxn_movement_type not ilike '702%%') and (ctxn_movement_type not ilike '322%%') and (ctxn_movement_type not ilike '102%%') and (ctxn_movement_type not ilike '309%%') and (ctxn_movement_type not ilike '350%%') and (ctxn_movement_type not ilike '343%%') and (ctxn_movement_type not ilike '321%%')"
        }
    },
    {
        "name": "goods_receipts",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "(ctxn_movement_type ilike '311%%') and (ctxn_primary_qty > 0) and (ctxn_handling_unit not ilike 'PLT%%')"
        }
    },
    {
        "name": "small_returns",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "(ctxn_movement_type ilike '261%%') and (ctxn_primary_qty < 0) and (ctxn_primary_qty >= -3)"
        }
    },
    {
        "name": "medium_returns",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "(ctxn_movement_type ilike '261%%') and (ctxn_primary_qty < -3) and (ctxn_primary_qty >= -50)"
        }
    },
    {
        "name": "large_returns",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "(ctxn_movement_type ilike '261%%') and (ctxn_primary_qty < -50)"
        }
    },
    {
        "name": "special_movements",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "5000",
            "where": "(ctxn_movement_type ilike '102%%' or ctxn_movement_type ilike '702%%')"
        }
    }
]

# ======================================
# FUNCIONES PRINCIPALES
# ======================================
def build_url(endpoint, params, warehouse_code):
    """Construye URL con codificación segura para todos los parámetros"""
    encoded_params = []
    base_where = f"ctxn_warehouse_code ilike '{warehouse_code}' and (ctxn_transaction_date > current_date -182)"
    
    for key, value in params.items():
        # Asegura que todos los valores sean strings
        str_key = str(key)
        str_value = str(value)
        
        # Manejo especial para el parámetro WHERE
        if key == "where":
            str_value = f"{base_where} and {value}"
        
        # Codificación URL segura
        encoded_key = quote(str_key)
        encoded_value = quote(str_value)
        
        encoded_params.append(f"{encoded_key}={encoded_value}")
    
    return f"{BASE_URL}{endpoint}?{'&'.join(encoded_params)}"

def fetch_api_data(url, query_name):
    """Obtiene datos con manejo robusto de errores"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"ℹ️  Consultando {query_name} (Intento {attempt + 1}/{MAX_RETRIES + 1})", end=" ", flush=True)
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()  # Lanza error para códigos 4XX/5XX
            
            data = response.json()
            if not data:
                print(f"⚠️  {query_name} devolvió datos vacíos")
                return pd.DataFrame()
                
            df = pd.json_normalize(data)
            df['load_timestamp'] = datetime.now().isoformat()
            df['query_name'] = query_name
            print(f"✅ {len(df)} registros obtenidos")
            return df
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                print(f"❌ {query_name} falló después de {MAX_RETRIES} reintentos: {str(e)}")
                return pd.DataFrame()
            print(f"⏳ Esperando {RETRY_DELAY}s antes de reintentar...")
            time.sleep(RETRY_DELAY)

def process_warehouse(warehouse_code):
    """Procesa todas las consultas para un almacén específico"""
    print(f"\n🔍 PROCESANDO ALMACÉN {warehouse_code}")
    all_data = pd.DataFrame()
    
    for config in QUERY_CONFIG:
        url = build_url(ENDPOINT, config["params"], warehouse_code)
        df = fetch_api_data(url, config["name"])
        
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        
        if config != QUERY_CONFIG[-1]:  # No esperar después de la última consulta
            print(f"⏳ Pausa de {REQUEST_DELAY}s entre consultas...")
            time.sleep(REQUEST_DELAY)
    
    return all_data

def save_data(df, warehouse_code):
    """Guarda el DataFrame en archivo Parquet"""
    if df.empty:
        print(f"❌ No hay datos para guardar del almacén {warehouse_code}")
        return False
    
    os.makedirs("data", exist_ok=True)
    filename = f"data/transactions_{warehouse_code}.parquet"
    
    try:
        df.to_parquet(filename, index=False)
        size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"\n💾 Datos guardados en {filename}")
        print(f"📊 Total registros: {len(df)}")
        print(f"📦 Tamaño del archivo: {size_mb:.2f} MB")
        return True
    except Exception as e:
        print(f"❌ Error guardando {filename}: {str(e)}")
        return False

# ======================================
# EJECUCIÓN PRINCIPAL
# ======================================
def main():
    """Función principal con manejo estructurado de errores"""
    print("\n🚀 INICIANDO RECOLECTOR DE DATOS")
    print(f"🔧 Configuración:\n- Almacenes: {WAREHOUSE_CODES}\n- BASE_URL: {'✅' if BASE_URL else '❌'}")
    
    start_time = time.time()
    
    try:
        for warehouse in WAREHOUSE_CODES:
            warehouse_data = process_warehouse(warehouse)
            save_data(warehouse_data, warehouse)
            
    except Exception as e:
        print(f"\n💥 ERROR CRÍTICO: {str(e)}")
        raise  # Propaga el error para que falle el workflow
    
    finally:
        duration = time.time() - start_time
        print(f"\n⌛ PROCESO COMPLETADO EN {duration:.2f} SEGUNDOS")

if __name__ == "__main__":
    main()
