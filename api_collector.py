import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# Configuración desde variables de entorno
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")  # Ahora es un secreto
HEADERS = {"token": TOKEN}
WAREHOUSE_CODES = os.getenv("WAREHOUSE_CODES", "1145,1290").split(",")
MAX_RETRIES = 2  # Exactamente 2 reintentos
REQUEST_DELAY = 30  # Segundos entre consultas
RETRY_DELAY = 10  # Segundos entre reintentos

def generate_urls(warehouse_code):
    """Genera URLs para el almacén específico con encoding seguro"""
    encoded_warehouse = quote(f"ctxn_warehouse_code ilike '{warehouse_code}%25%'")
    
    queries = [
        {"take": 30000, "conditions": "ctxn_movement_type ilike '313%25%' and (ctxn_primary_qty > 0) and (ctxn_primary_qty <= 3)"},
        {"take": 30000, "conditions": "ctxn_movement_type ilike '313%25%' and (ctxn_primary_qty > 3) and (ctxn_primary_qty <= 50)"},
        {"take": 30000, "conditions": "ctxn_movement_type ilike '313%25%' and (ctxn_primary_qty > 50)"},
        {"take": 20000, "conditions": "(ctxn_movement_type not ilike '313%25%') and (ctxn_movement_type not ilike '311%25%') and (ctxn_movement_type not ilike '261%25%') and (ctxn_movement_type not ilike '344%25%') and (ctxn_movement_type not ilike '327%25%') and (ctxn_movement_type not ilike '349%25%') and (ctxn_movement_type not ilike '325%25%') and (ctxn_movement_type not ilike '702%25%') and (ctxn_movement_type not ilike '322%25%') and (ctxn_movement_type not ilike '102%25%') and (ctxn_movement_type not ilike '309%25%') and (ctxn_movement_type not ilike '350%25%') and (ctxn_movement_type not ilike '343%25%') and (ctxn_movement_type not ilike '321%25%')"},
        {"take": 30000, "conditions": "(ctxn_movement_type ilike '311%25%') and (ctxn_primary_qty > 0) and (ctxn_handling_unit not ilike 'PLT%25%')"},
        {"take": 30000, "conditions": "(ctxn_movement_type ilike '261%25%') and (ctxn_primary_qty < 0) and (ctxn_primary_qty >= -3)"},
        {"take": 30000, "conditions": "(ctxn_movement_type ilike '261%25%') and (ctxn_primary_qty < -3) and (ctxn_primary_qty >= -50)"},
        {"take": 30000, "conditions": "(ctxn_movement_type ilike '261%25%') and (ctxn_primary_qty < -50)"},
        {"take": 5000, "conditions": "(ctxn_movement_type ilike '102%25%' or ctxn_movement_type ilike '702%25%')"}
    ]
    
    base_conditions = f"(ctxn_transaction_date > current_date - 182)"
    urls = []
    
    for query in queries:
        url = (
            f"{BASE_URL}?orderby=ctxn_transaction_date%20desc"
            f"&take={query['take']}"
            f"&where={encoded_warehouse}%20and%20{base_conditions}%20and%20{query['conditions']}"
        )
        urls.append(url)
    
    return urls

def fetch_api_data(url, warehouse_code):
    """Obtiene datos con manejo de errores y 2 reintentos exactos"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            df = pd.json_normalize(response.json())
            df['data_source'] = f"warehouse_{warehouse_code}"
            df['load_timestamp'] = datetime.now().isoformat()
            df['warehouse_code'] = warehouse_code
            
            return df
            
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"⚠️ Error después de {MAX_RETRIES} reintentos: {str(e)}")
                return pd.DataFrame()
            print(f"⚡ Reintento {attempt + 1} de {MAX_RETRIES}...")
            time.sleep(RETRY_DELAY)

def process_warehouse(warehouse_code):
    """Procesa todas las consultas para un almacén"""
    print(f"\n🔍 Iniciando almacén {warehouse_code}")
    urls = generate_urls(warehouse_code)
    all_data = pd.DataFrame()
    
    for idx, url in enumerate(urls, 1):
        print(f"📊 Consulta {idx}/{len(urls)}")
        df = fetch_api_data(url, warehouse_code)
        
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        
        if idx < len(urls):
            print(f"⏳ Pausa de {REQUEST_DELAY}s...")
            time.sleep(REQUEST_DELAY)
    
    return all_data

def save_data(df, warehouse_code):
    """Guarda los datos en formato Parquet"""
    if df.empty:
        print(f"❌ Sin datos para {warehouse_code}")
        return None
    
    os.makedirs("data", exist_ok=True)
    filename = f"data/transactions_{warehouse_code}.parquet"
    
    try:
        df.to_parquet(filename, index=False)
        print(f"✅ Datos guardados: {filename} ({len(df)} registros)")
        return filename
    except Exception as e:
        print(f"❌ Error guardando {filename}: {str(e)}")
        return None

def main():
    """Función principal con reporting mejorado"""
    print("🚀 Iniciando recolector de datos")
    start_time = time.time()
    
    try:
        for code in WAREHOUSE_CODES:
            data = process_warehouse(code)
            save_data(data, code)
            
    except Exception as e:
        print(f"💥 Error crítico: {str(e)}")
        raise
    
    finally:
        duration = time.time() - start_time
        print(f"⌛ Proceso completado en {duration:.2f} segundos")

if __name__ == "__main__":
    main()
