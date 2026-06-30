import boto3
import pandas as pd
from datetime import datetime
from kafka_producer import fila_kafka

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'alfabetizacao-datalake'

def processa_evento_streaming():
    #Consome eventos da fila e salva no S3
    print("Consumidor iniciado — aguardando eventos...\n")
    
    while not fila_kafka.empty():
        evento = fila_kafka.get()
        
        agora = datetime.now()
        year = agora.year
        month = str(agora.month).zfill(2)
        timestamp = agora.strftime("%Y%m%d_%H%M%S_%f")
        
        df = pd.DataFrame([evento])
        
        caminho_local = f"temp_streaming_{timestamp}.parquet"
        caminho_s3 = f"bronze/streaming/year={year}/month={month}/evento_{timestamp}.parquet"
        
        # Salva localmente
        df.to_parquet(caminho_local)
        
        # Sobe pro S3
        s3.upload_file(caminho_local, BUCKET, caminho_s3)
        
        # Remove arquivo temporário
        import os
        os.remove(caminho_local)
        
        print(f"[Consumidor] Evento salvo: {caminho_s3}")

    print("\nFila vazia — consumidor finalizado.")