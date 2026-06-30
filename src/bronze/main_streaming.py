import threading
from kafka_producer import produtor
from ingest_streaming import processa_evento_streaming

def main():
    print("=== Iniciando simulação Kafka ===\n")

    # Thread do produtor que gera eventos em paralelo
    thread_produtor = threading.Thread(target=produtor, args=(20, 1))

    # Inicia o produtor
    thread_produtor.start()

    # Aguarda o produtor terminar antes de consumir
    thread_produtor.join()

    # Processa todos os eventos acumulados na fila
    processa_evento_streaming()

    print("\n=== Simulação finalizada ===")

if __name__ == "__main__":
    main()