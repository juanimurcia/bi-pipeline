import requests
import os

def test_extraction():
    print("--- INICIANDO FASE DE EXTRACCIÓN ---")
    # Probamos una API real (Tipos de cambio)
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    if response.status_code == 200:
        print("✅ Extracción exitosa: Datos recibidos de la API.")
        return response.json()
    else:
        raise Exception("❌ Fallo en la extracción.")

def test_medallion_flow(data):
    # Simulación de Capa Bronze
    print("--- SIMULANDO CAPA BRONZE ---")
    print(f"Dato crudo recibido: {list(data.keys())[:3]}...") 
    
    # Simulación de Capa Silver
    print("--- SIMULANDO CAPA SILVER ---")
    tasa_ars = data['rates'].get('ARS')
    print(f"Dato transformado: Tasa de cambio ARS = {tasa_ars}")
    
    # Simulación de Capa Gold
    print("--- SIMULANDO CAPA GOLD ---")
    print(f"KPI Calculado: El valor del dólar hoy es {tasa_ars} pesos.")
    print("✅ Pipeline completado lógicamente.")

if __name__ == "__main__":
    raw_data = test_extraction()
    test_medallion_flow(raw_data)
