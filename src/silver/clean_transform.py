import boto3
import pandas as pd
from io import BytesIO
import os

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'alfabetizacao-datalake'

# Funções utilitárias

def le_parquet_s3(caminho_s3):
    # Lê arquivo Parquet do S3
    obj = s3.get_object(Bucket=BUCKET, Key=caminho_s3)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def salva_parquet_s3(df, caminho_s3):
    # Salva o DataFrame como Parquet no S3
    caminho_local = f"temp_{caminho_s3.replace('/', '_')}"
    df.to_parquet(caminho_local, index=False)
    s3.upload_file(caminho_local, BUCKET, caminho_s3)
    os.remove(caminho_local)
    print(f"✓ Salvo: {caminho_s3}")

def dropa_duplicadas(df):
    antes = df.shape[0]
    df = df.drop_duplicates()
    depois = df.shape[0]
    print(f"  Duplicatas removidas: {antes - depois}")
    return df

# Leitura da camada Bronze

print("Lendo bases da camada Bronze\n")

anos = [2023, 2024]

uf = pd.concat([le_parquet_s3(f"bronze/uf/year={ano}/dados.parquet") for ano in anos])
municipio = pd.concat([le_parquet_s3(f"bronze/municipio/year={ano}/dados.parquet") for ano in anos])
meta_brasil = pd.concat([le_parquet_s3(f"bronze/meta_alfabetizacao_brasil/year={ano}/dados.parquet") for ano in anos])
meta_uf = pd.concat([le_parquet_s3(f"bronze/meta_alfabetizacao_uf/year={ano}/dados.parquet") for ano in anos])
meta_municipio = pd.concat([le_parquet_s3(f"bronze/meta_alfabetizacao_municipio/year={ano}/dados.parquet") for ano in anos])
alunos = pd.concat([le_parquet_s3(f"bronze/alunos/year={ano}/dados.parquet") for ano in anos])

# Limpeza: Base Alunos

print("\n--- Tratando base Alunos ---")

# As colunas da base "alunos" precisaram ser renomeadas pois a base tratada estava indisponível no site
colunas_alunos = {
    'NU_ANO_AVALIACAO': 'ano',
    'SG_UF': 'sigla_uf',
    'CO_MUNICIPIO': 'id_municipio',
    'TP_DEPENDENCIA': 'rede',
    'TP_SERIE': 'serie'
}
alunos = alunos.rename(columns=colunas_alunos)

# Converte rede de inteiro para texto
# A informação do DE-PARA foi retirada do dicionário de dados que vem em conjunto com a base
mapa_rede_alunos = {1: 'Federal', 2: 'Estadual', 3: 'Municipal', 4: 'Privada'}
alunos['rede'] = alunos['rede'].map(mapa_rede_alunos)

# Remove rede Privada pois não tem meta no projeto
alunos = alunos[alunos['rede'] != 'Privada']
print(f"  Após remover Privada: {alunos.shape[0]} registros")

# Remove alunos ausentes pois o indicador mede apenas alunos avaliados
alunos = alunos[alunos['IN_PRESENCA_LP'] == 1]
print(f"  Após remover ausentes: {alunos.shape[0]} registros")

# Corrige tipo de IN_ALFABETIZADO
alunos['IN_ALFABETIZADO'] = alunos['IN_ALFABETIZADO'].astype(int)

alunos = dropa_duplicadas(alunos)

# Limpeza: Base UF

print("\n--- Tratando base UF ---")

# Remove registro com rede = 0 (valor inválido com base no dicionário de dados)
uf = uf[uf['rede'] != 0]
print(f"  Após remover rede=0: {uf.shape[0]} registros")

# Converte rede de inteiro para texto (confirmado pelo dicionário oficial)
mapa_rede_agregado = {2: 'Estadual', 3: 'Municipal', 5: 'Pública'}
uf['rede'] = uf['rede'].map(mapa_rede_agregado)

uf = dropa_duplicadas(uf)

# Limpeza: Base Município

print("\n--- Tratando base Município ---")

# Remove registros com rede = 0 (valor inválido com base no dicionário de dados)
municipio = municipio[municipio['rede'] != 0]
print(f"  Após remover rede=0: {municipio.shape[0]} registros")

# Converte rede de inteiro para texto
municipio['rede'] = municipio['rede'].map(mapa_rede_agregado)

municipio = dropa_duplicadas(municipio)

# Limpeza: Bases Meta

print("\n--- Tratando bases Meta ---")

# Padroniza meta_alfabetizacao_2030 para float (inconsistência de tipo na fonte)
meta_municipio['meta_alfabetizacao_2030'] = meta_municipio['meta_alfabetizacao_2030'].astype(float)
meta_brasil['meta_alfabetizacao_2030'] = meta_brasil['meta_alfabetizacao_2030'].astype(float)

meta_brasil = dropa_duplicadas(meta_brasil)
meta_uf = dropa_duplicadas(meta_uf)
meta_municipio = dropa_duplicadas(meta_municipio)

# Salvar as bases tratadas na camada Silver

print("\nSalvando bases na Silver\n")

bases = {
    'uf': uf,
    'municipio': municipio,
    'meta_alfabetizacao_brasil': meta_brasil,
    'meta_alfabetizacao_uf': meta_uf,
    'meta_alfabetizacao_municipio': meta_municipio,
    'alunos': alunos
}

for nome, df in bases.items():
    for ano in anos:
        df_ano = df[df['ano'] == ano]
        salva_parquet_s3(df_ano, f"silver/{nome}/year={ano}/dados.parquet")

print("\n✓ Camada Silver concluída!")