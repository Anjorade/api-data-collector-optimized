import os
import requests
import pandas as pd
import time
from datetime import datetime

# Configuración desde secrets
TOKEN = os.getenv("API_TOKEN")
HEADERS = {"token": TOKEN}
BASE_URL = os.getenv("API_BASE_URL")  # Ahora viene de secrets

def generate_urls(warehouse_code):
    """Genera las URLs para un código de almacén específico"""
    return [
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20ctxn_movement_type%20ilike%20'313%25%25'%20and%20(ctxn_primary_qty%20%3E%200)%20and%20%20(ctxn_primary_qty%20%3C%3D%203)",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20ctxn_movement_type%20ilike%20'313%25%25'%20and%20(ctxn_primary_qty%20%3E%203)%20and%20%20(ctxn_primary_qty%20%3C%3D%2050)",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20ctxn_movement_type%20ilike%20'313%25%25'%20and%20(ctxn_primary_qty%20%3E%2050)",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=20000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20(ctxn_movement_type%20not%20ilike%20'313%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'311%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'261%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'344%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'327%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'349%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'325%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'702%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'322%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'102%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'309%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'350%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'343%25%25')%20and%20(ctxn_movement_type%20not%20ilike%20'321%25%25')",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20(ctxn_movement_type%20ilike%20'311%25%25')%20and%20(ctxn_primary_qty%20%3E%200)%20and%20(ctxn_handling_unit%20not%20ilike%20'PLT%25%25')",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20(ctxn_movement_type%20ilike%20'261%25%25')%20and%20(ctxn_primary_qty%20%3C%200)%20and%20(ctxn_primary_qty%20%3E%3D%20-3)",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20(ctxn_movement_type%20ilike%20'261%25%25')%20and%20(ctxn_primary_qty%20%3C%20-3)%20and%20(ctxn_primary_qty%20%3E%3D%20-50)",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=30000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20(ctxn_movement_type%20ilike%20'261%25%25')%20and%20(ctxn_primary_qty%20%3C%20-50)",
        f"{BASE_URL}?orderby=ctxn_transaction_date%20desc&take=5000&where=ctxn_warehouse_code%20ilike%20'{warehouse_code}'%20and%20(ctxn_transaction_date%20%3E%20current_date%20-182)%20and%20(ctxn_movement_type%20ilike%20'102%25%25'%20or%20ctxn_movement_type%20ilike%20'702%25%25')"
    ]

def fetch_api_data(url):
    """Obtiene datos de la API con manejo de errores"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Convertir a DataFrame y agregar metadatos
        df = pd.json_normalize(data)
        df['data_source'] = url.split('where=')[1][:30] + "..."
        df['load_timestamp'] = datetime.now().isoformat()
        df['warehouse_code'] = url.split("ilike%20'")[1][:4]  # Extraer código de almacén
        
        return df
    except Exception as e:
        print(f"Error al consultar {url}: {str(e)}")
        return pd.DataFrame()

def process_warehouse(warehouse_code):
    """Procesa todas las URLs para un código de almacén específico"""
    urls = generate_urls(warehouse_code)
    all_data = pd.DataFrame()
    
    for i, url in enumerate(urls):
        print(f"Consultando URL {i+1}/{len(urls)} para almacén {warehouse_code}: {url[:50]}...")
        df = fetch_api_data(url)
        
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        
        if i < len(urls) - 1:
            print("Esperando 30 segundos entre requests...")
            time.sleep(30)
    
    return all_data

def save_data(df, warehouse_code):
    """Guarda los datos en formato parquet"""
    if not df.empty:
        os.makedirs("data", exist_ok=True)
        filename = f"data/transactions_{warehouse_code}.parquet"
        df.to_parquet(filename, index=False)
        print(f"Datos guardados en {filename}. Total de registros: {len(df)}")
        return True
    else:
        print(f"No se obtuvieron datos para el almacén {warehouse_code}")
        return False

def main():
    # Procesar ambos almacenes
    warehouse_codes = ['1145', '1290']
    success_count = 0
    
    for code in warehouse_codes:
        print(f"\n{'='*50}")
        print(f"INICIANDO PROCESO PARA ALMACÉN {code}")
        print(f"{'='*50}\n")
        
        df = process_warehouse(code)
        if save_data(df, code):
            success_count += 1
        
        # Pequeña pausa entre almacenes
        if code != warehouse_codes[-1]:
            print("\nEsperando 30 segundos antes del próximo almacén...")
            time.sleep(30)
    
    if success_count == 0:
        raise Exception("Todas las llamadas API fallaron para ambos almacenes")

if __name__ == "__main__":
    main()
