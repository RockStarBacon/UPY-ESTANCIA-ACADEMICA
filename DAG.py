from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

estaciones = [
    "ICIUDA228", "ITAMPI10", "ITAMPI7", "ICIUDA207", "ITAMPI4",
    "ICIUDA234", "ITAMPI9", "ICIUDA235", "IMIRAM24", "IMIRAM25",
    "IALTAM36", "IALTAM12", "IALTAM35", "IALTAM33"
]

direccion_a_angulo = {
    'North': 0, 'NNE': 23, 'NE': 45, 'ENE': 68, 'East': 90,
    'ESE': 113, 'SE': 135, 'SSE': 158, 'South': 180, 'SSW': 203,
    'SW': 225, 'WSW': 248, 'West': 270, 'WNW': 293, 'NW': 315, 'NNW': 338,
    'N': 0, 'E': 90, 'S': 180, 'W': 270
}

def extraer_y_guardar():
    engine = create_engine("postgresql+psycopg2://postgres:postgres@postgres:5432/prueba_pipeline")
    ahora = datetime.now()
    fecha_str = ahora.strftime('%Y-%m-%d')

    for estacion in estaciones:
        url = f"https://www.wunderground.com/dashboard/pws/{estacion}/table/{fecha_str}/{fecha_str}/daily"

        try:
            r = requests.get(url, timeout=10)
            soup = BeautifulSoup(r.content, 'html.parser')
            tabla = soup.find('table', class_='history-table desktop-table')
            if not tabla:
                print(f"No hay datos para estación: {estacion}")
                continue

            rows = tabla.find('tbody').find_all('tr')
            if not rows:
                continue

            tiempo, temperatura, rocio, humedad, viento, velocidad, rafaga = [], [], [], [], [], [], []
            presion, tasa, acumulacion, uv, solar = [], [], [], [], []

            for row in rows:
                columnas = row.find_all('td')
                if len(columnas) < 12:
                    continue
                tiempo.append(columnas[0].get_text())
                temperatura.append(columnas[1].get_text())
                rocio.append(columnas[2].get_text())
                humedad.append(columnas[3].get_text())
                viento.append(columnas[4].get_text())
                velocidad.append(columnas[5].get_text())
                rafaga.append(columnas[6].get_text())
                presion.append(columnas[7].get_text())
                tasa.append(columnas[8].get_text())
                acumulacion.append(columnas[9].get_text())
                uv.append(columnas[10].get_text())
                solar.append(columnas[11].get_text())

            df = pd.DataFrame({
                'datetime_medicion': [
                    datetime.combine(ahora.date(), datetime.strptime(t, '%I:%M %p').time())
                    for t in tiempo
                ],
                'hora': [datetime.strptime(t, '%I:%M %p').strftime('%H:%M') for t in tiempo],
                'temperatura': [(float(t.replace('\xa0°F', '').strip()) - 32) / 1.8 if '\xa0°F' in t else None for t in temperatura],
                'rocio': [(float(d.replace('\xa0°F', '').strip()) - 32) / 1.8 if '\xa0°F' in d else None for d in rocio],
                'humedad': [int(h.replace('\xa0°%', '').strip()) if '\xa0°%' in h else None for h in humedad],
                'viento': viento,
                'direccion': [direccion_a_angulo.get(v.strip(), 0) for v in viento],
                'velocidad': [float(v.replace('\xa0°mph', '').strip()) * 1.60934 if '\xa0°mph' in v else 0 for v in velocidad],
                'rafaga': [float(g.replace('\xa0°mph', '').strip()) * 1.60934 if '\xa0°mph' in g else 0 for g in rafaga],
                'presion': [float(p.replace('\xa0°in', '').strip()) * 33.86389 if '\xa0°in' in p else 0 for p in presion],
                'tasa': [float(x.replace('\xa0°in', '').strip()) * 25.4 if '\xa0°in' in x else 0 for x in tasa],
                'acumulacion': [float(x.replace('\xa0°in', '').strip()) * 25.4 if '\xa0°in' in x else 0 for x in acumulacion],
                'uv': [int(float(u.strip())) if u.replace('.', '', 1).isdigit() else 0 for u in uv],
                'solar': [float(s.strip()) if s.replace('.', '', 1).isdigit() else 0 for s in solar],
                'id_estacion_clima': estacion
            })

            # Leer fechas ya insertadas
            existing = pd.read_sql(f"""
                SELECT datetime_medicion FROM medicion_meteorologica
                WHERE id_estacion_clima = '{estacion}'
            """, engine)

            df_nuevo = df[~df['datetime_medicion'].isin(existing['datetime_medicion'])]

            if not df_nuevo.empty:
                df_nuevo.to_sql('medicion_meteorologica', engine, if_exists='append', index=False)
                print(f"✔ Insertados {len(df_nuevo)} datos nuevos de {estacion}")
            else:
                print(f"⏩ Sin nuevos datos para {estacion}")

        except Exception as e:
            print(f"❌ Error en estación {estacion}: {e}")

# Definición del DAG
with DAG(
    dag_id='live_meteorological_scraper',
    default_args=default_args,
    start_date=datetime(2025, 8, 4),
    schedule_interval='*/5 * * * *',  # cada 5 minutos
    catchup=False,
    tags=['clima', 'scraping', 'en_vivo']
) as dag:
    ejecutar_scraping = PythonOperator(
        task_id='extraer_e_insertar_meteorologia',
        python_callable=extraer_y_guardar
    )
