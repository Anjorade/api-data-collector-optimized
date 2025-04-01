import os
import requests
import pandas as pd
import time
from datetime import datetime

# Configuración desde variables de entorno
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")  # Debe configurarse como secret
HEADERS = {"token": TOKEN}
WAREHOUSE_CODES = os.getenv("WAREHOUSE_CODES", "1145,1290").split(",")
MAX_RETRIES = 2
REQUEST_DELAY = 30
RETRY_DELAY = 10

# Validación crítica de variables
if not all([TOKEN, BASE_URL]):
    raise ValueError(
        "❌ Variables críticas no configuradas. Verifica:\n"
        f"- API_TOKEN: {'✅' if TOKEN else '❌'}\n"
        f"- API_BASE_URL: {'✅' if BASE_URL else '❌'}\n"
        "Asegúrate de configurarlas como secrets/variables de entorno."
    )

def generate_urls(warehouse_code):
    """Genera URLs para el almacén específico"""
    conditions = [
        {"take": 30000, "where": "ctxn_movement_type ilike '313%%' and (ctxn_primary_qty > 0) and (ctxn_primary_qty <= 3)"},
        {"take": 30000, "where": "ctxn_movement_type ilike '313%%' and (ctxn_primary_qty > 3) and (ctxn_primary_qty <= 50)"},
        {"take": 30000, "where": "ctxn_movement_type ilike '313%%' and (ctxn_primary_qty > 50)"},
        {"take": 20000, "where": "(ctxn_movement_type not ilike '313%%') and (ctxn_movement_type not ilike '311%%') and (ctxn_movement_type not ilike '261%%') and (ctxn_movement_type not ilike '344%%') and (ctxn_movement_type not ilike '327%%') and (ctxn_movement_type not ilike '349%%') and (ctxn_movement_type not ilike '325%%') and (ctxn_movement_type not ilike '702%%') and (ctxn_movement_type not ilike '322%%') and (ctxn_movement_type not ilike '102%%') and (ctxn_movement_type not ilike '309%%') and (ctxn_movement_type not ilike '350%%') and (ctxn_movement_type not ilike '343%%') and (ctxn_movement_type not ilike '321%%')"},
        {"take": 30000, "where": "(ctxn_movement_type ilike '311%%') and (ctxn_primary_qty > 0) and (ctxn_handling_unit not ilike 'PLT%%')"},
        {"take": 30000, "where": "(ctxn_movement_type ilike '261%%') and (ctxn_primary_qty < 0) and (ctxn_primary_qty >= -3)"},
        {"take": 30000, "where": "(ctxn_movement_type ilike '261%%') and (ctxn_primary_qty < -3) and (ctxn_primary_qty >= -50)"},
        {"take": 30000, "where": "(ctxn_movement_type ilike '261%%') and (ctxn_primary_qty < -50)"},
        {"take": 5000, "where": "(ctxn_movement_type ilike '102%%' or ctxn_movement_type ilike '702%%')"}
    ]
    
    base_where = f"ctxn_warehouse_code ilike '{warehouse_code}' and (ctxn_transaction_date > current_date -182)"
    return [
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take={q['take']}&where={base_where}%20and%20{q['where']}"
        for q in conditions
    ]

def fetch_api_data(url):
    """Obtiene datos con manejo de errores"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return pd.json_normalize(response.json())
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"⚠️ Error en URL después de {MAX_RETRIES} reintentos: {url[:50]}...")
                return pd.DataFrame()
            time.sleep(RETRY_DELAY)

def main():
    """Función principal con reporting mejorado"""
    print("🚀 Iniciando recolector de datos")
    print(f"🔧 Configuración:\n- Almacenes: {WAREHOUSE_CODES}\n- BASE_URL: {'✅' if BASE_URL else '❌'}")
    
    start_time = time.time()
    try:
        for code in WAREHOUSE_CODES:
            print(f"\n🔍 Procesando almacén {code}")
            urls = generate_urls(code)
            all_data = pd.DataFrame()
            
            for idx, url in enumerate(urls, 1):
                print(f"📦 Consulta {idx}/{len(urls)}...", end=" ", flush=True)
                df = fetch_api_data(url)
                if not df.empty:
                    all_data = pd.concat([all_data, df], ignore_index=True)
                    print(f"✅ {len(df)} registros")
                else:
                    print("❌ Sin datos")
                
                if idx < len(urls):
                    time.sleep(REQUEST_DELAY)
            
            if not all_data.empty:
                os.makedirs("data", exist_ok=True)
                filename = f"data/transactions_{code}.parquet"
                all_data.to_parquet(filename, index=False)
                print(f"💾 Guardado: {filename} ({len(all_data)} registros)")
                
    except Exception as e:
        print(f"\n💥 Error crítico: {str(e)}")
        raise
    finally:
        print(f"\n⌛ Tiempo total: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
