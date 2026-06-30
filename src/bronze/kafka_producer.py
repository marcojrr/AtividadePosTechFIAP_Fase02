import pandas as pd
import random
import time
import json
from queue import Queue

# Carrega os dados reais para a simulação ficar realista
alunos = pd.read_csv('data/raw/alunos.csv', usecols=['SG_UF', 'CO_MUNICIPIO'])

# Monta dicionário UF com lista de municípios
municipios_por_uf = (
    alunos
    .drop_duplicates(subset=['SG_UF', 'CO_MUNICIPIO'])
    .groupby('SG_UF')['CO_MUNICIPIO']
    .apply(list)
    .to_dict()
)

# Cria uma fila para simular o envio de eventos para o Kafka
fila_kafka = Queue()

# Gera um evento aleatório com base nos dados reais
def gera_evento():
    uf = random.choice(list(municipios_por_uf.keys()))
    municipio = random.choice(municipios_por_uf[uf])
    proficiencia = round(random.uniform(400, 950), 2)

    return {
        "NU_ANO_AVALIACAO": random.choice([2025, 2026]),
        "SG_UF": uf,
        "CO_MUNICIPIO": municipio,
        "ID_ALUNO": random.randint(10000000, 99999999),
        "ID_ESCOLA": random.randint(10000000, 99999999),
        "VL_PROFICIENCIA_LP": proficiencia,
        "IN_ALFABETIZADO": 1 if proficiencia >= 743 else 0
    }

# Cria o produtor de eventos, que gera e envia eventos para a fila Kafka
def produtor(n_eventos=20, intervalo_segundos=1):
    print(f"Iniciando produtor — gerando {n_eventos} eventos...\n")
    for i in range(n_eventos):
        evento = gera_evento()
        fila_kafka.put(evento)
        print(f"[Evento {i+1}] Produzido: Aluno {evento['ID_ALUNO']} | UF: {evento['SG_UF']} | Alfabetizado: {evento['IN_ALFABETIZADO']}")
        time.sleep(intervalo_segundos)
    print("\nProdutor finalizado.")
