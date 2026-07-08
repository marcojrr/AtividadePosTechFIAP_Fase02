import boto3
import pandas as pd
from io import BytesIO
import os
import numpy as np

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'alfabetizacao-datalake'

# Funções utilitárias

def le_parquet_s3(caminho_s3):
    obj = s3.get_object(Bucket=BUCKET, Key=caminho_s3)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def salva_parquet_s3(df, caminho_s3):
    caminho_local = f"temp_{caminho_s3.replace('/', '_')}"
    df.to_parquet(caminho_local, index=False)
    s3.upload_file(caminho_local, BUCKET, caminho_s3)
    os.remove(caminho_local)
    print(f"✓ Salvo: {caminho_s3}")

# Leitura da Silver

print("Lendo bases da Silver...\n")

anos = [2023, 2024]

integrado = pd.concat([le_parquet_s3(f"silver/integrado/year={ano}/dados.parquet") for ano in anos])
meta_municipio = pd.concat([le_parquet_s3(f"silver/meta_alfabetizacao_municipio/year={ano}/dados.parquet") for ano in anos])
meta_uf = pd.concat([le_parquet_s3(f"silver/meta_alfabetizacao_uf/year={ano}/dados.parquet") for ano in anos])
uf = pd.concat([le_parquet_s3(f"silver/uf/year={ano}/dados.parquet") for ano in anos])

#=============================TABELA 1: indicadores_municipio=============================
print("Calculando indicadores por município...\n")

ind_municipio = (
    integrado
    .groupby(['ano', 'id_municipio', 'rede'])
    .agg(
        taxa_real_calculada=('in_alfabetizado', lambda x: round(x.mean() * 100, 2)),
        media_proficiencia_lp=('vl_proficiencia_lp', lambda x: round(x.mean(), 2)),
        total_alunos_avaliados=('id_aluno', 'count'),
        mun_taxa_alfabetizacao=('mun_taxa_alfabetizacao', 'first')
    )
    .reset_index()
)

# Adiciona sigla_uf com join integrado
sigla_municipio = integrado[['id_municipio', 'sigla_uf']].drop_duplicates()
ind_municipio = pd.merge(ind_municipio, sigla_municipio, on='id_municipio', how='left')

# Adiciona metas por município
meta_municipio_sel = meta_municipio[[
    'ano', 'id_municipio', 'rede',
    'meta_alfabetizacao_2024', 'meta_alfabetizacao_2025',
    'meta_alfabetizacao_2026', 'meta_alfabetizacao_2027',
    'meta_alfabetizacao_2028', 'meta_alfabetizacao_2029',
    'meta_alfabetizacao_2030'
]].rename(columns={
    'meta_alfabetizacao_2024': 'meta_mun_alf_2024',
    'meta_alfabetizacao_2025': 'meta_mun_alf_2025',
    'meta_alfabetizacao_2026': 'meta_mun_alf_2026',
    'meta_alfabetizacao_2027': 'meta_mun_alf_2027',
    'meta_alfabetizacao_2028': 'meta_mun_alf_2028',
    'meta_alfabetizacao_2029': 'meta_mun_alf_2029',
    'meta_alfabetizacao_2030': 'meta_mun_alf_2030'
})

ind_municipio = pd.merge(
    ind_municipio,
    meta_municipio_sel,
    on=['ano', 'id_municipio', 'rede'],
    how='left'
)

# Indicadores derivados
ind_municipio['gap_meta_2024'] = round(
    ind_municipio['taxa_real_calculada'] - ind_municipio['meta_mun_alf_2024'], 2
)
ind_municipio['flag_risco'] = (ind_municipio['taxa_real_calculada'] < 50).astype(int)
ind_municipio['anos_restantes'] = 2030 - ind_municipio['ano']
ind_municipio['crescimento_anual_necessario'] = round(
    (ind_municipio['meta_mun_alf_2030'] - ind_municipio['taxa_real_calculada']) / ind_municipio['anos_restantes'], 2
)

print(f"  Municípios em risco (taxa < 50%): {ind_municipio[ind_municipio['flag_risco'] == 1].shape[0]}")
print(f"  Total de registros: {ind_municipio.shape[0]}")

#=============================TABELA 2: indicadores_uf=============================

print("\nCalculando indicadores por UF...\n")

# Calcula por Estadual e Municipal separadamente
ind_uf = (
    integrado
    .groupby(['ano', 'sigla_uf', 'rede'])
    .agg(
        taxa_real_calculada=('in_alfabetizado', lambda x: round(x.mean() * 100, 2)),
        media_proficiencia_lp=('vl_proficiencia_lp', lambda x: round(x.mean(), 2)),
        total_alunos_avaliados=('id_aluno', 'count')
    )
    .reset_index()
)

# Calcula agregado Pública (Estadual + Municipal juntos)
ind_uf_publica = (
    integrado[integrado['rede'].isin(['Estadual', 'Municipal'])]
    .groupby(['ano', 'sigla_uf'])
    .agg(
        taxa_real_calculada=('in_alfabetizado', lambda x: round(x.mean() * 100, 2)),
        media_proficiencia_lp=('vl_proficiencia_lp', lambda x: round(x.mean(), 2)),
        total_alunos_avaliados=('id_aluno', 'count')
    )
    .reset_index()
)
ind_uf_publica['rede'] = 'Pública'

# Concatena Estadual, Municipal e Pública
ind_uf = pd.concat([ind_uf, ind_uf_publica], ignore_index=True)

# Adiciona taxa oficial da UF
uf_sel = uf[['ano', 'sigla_uf', 'rede', 'taxa_alfabetizacao', 'media_portugues']].rename(columns={
    'taxa_alfabetizacao': 'uf_taxa_alfabetizacao',
    'media_portugues':    'uf_media_portugues'
})
ind_uf = pd.merge(ind_uf, uf_sel, on=['ano', 'sigla_uf', 'rede'], how='left')

# Adiciona metas por UF só bate com rede='Pública'
meta_uf_sel = meta_uf[[
    'ano', 'sigla_uf', 'rede',
    'meta_alfabetizacao_2024', 'meta_alfabetizacao_2030'
]].rename(columns={
    'meta_alfabetizacao_2024': 'meta_uf_alf_2024',
    'meta_alfabetizacao_2030': 'meta_uf_alf_2030'
})
ind_uf = pd.merge(ind_uf, meta_uf_sel, on=['ano', 'sigla_uf', 'rede'], how='left')

# Gap da meta — só calculado para rede Pública
ind_uf['gap_meta_2024'] = round(
    ind_uf['taxa_real_calculada'] - ind_uf['meta_uf_alf_2024'], 2
)

# Desigualdade interna por UF
desigualdade_uf = (
    integrado[['ano', 'sigla_uf', 'id_municipio', 'rede']]
    .drop_duplicates()
    .merge(ind_municipio[['ano', 'id_municipio', 'rede', 'taxa_real_calculada']], on=['ano', 'id_municipio', 'rede'])
    .groupby(['ano', 'sigla_uf', 'rede'])
    .agg(
        max_taxa_municipio=('taxa_real_calculada', 'max'),
        min_taxa_municipio=('taxa_real_calculada', 'min')
    )
    .reset_index()
)
desigualdade_uf['desigualdade_interna'] = round(
    desigualdade_uf['max_taxa_municipio'] - desigualdade_uf['min_taxa_municipio'], 2
)

ind_uf = pd.merge(ind_uf, desigualdade_uf, on=['ano', 'sigla_uf', 'rede'], how='left')

print(f"  Total de registros: {ind_uf.shape[0]}")

#=============================TABELA 3: evolucao_temporal=============================

print("\nCalculando evolução temporal...\n")

evolucao = ind_municipio[['ano', 'id_municipio', 'rede', 'taxa_real_calculada', 'meta_mun_alf_2024']].copy()

evolucao_pivot = evolucao.pivot_table(
    index=['id_municipio', 'rede'],
    columns='ano',
    values='taxa_real_calculada'
).reset_index()

evolucao_pivot.columns = ['id_municipio', 'rede', 'taxa_2023', 'taxa_2024']

meta_ref = meta_municipio_sel[meta_municipio_sel['ano'] == 2024][['id_municipio', 'rede', 'meta_mun_alf_2024']]
evolucao_pivot = pd.merge(evolucao_pivot, meta_ref, on=['id_municipio', 'rede'], how='left')

evolucao_pivot['variacao'] = round(evolucao_pivot['taxa_2024'] - evolucao_pivot['taxa_2023'], 2)
evolucao_pivot['atingiu_meta_2023'] = (evolucao_pivot['taxa_2023'] >= evolucao_pivot['meta_mun_alf_2024']).astype(int)
evolucao_pivot['atingiu_meta_2024'] = (evolucao_pivot['taxa_2024'] >= evolucao_pivot['meta_mun_alf_2024']).astype(int)
evolucao_pivot['melhorou'] = (evolucao_pivot['variacao'] > 0).astype(int)

print(f"  Municípios que melhoraram: {evolucao_pivot[evolucao_pivot['melhorou'] == 1].shape[0]}")
print(f"  Municípios que pioraram: {evolucao_pivot[evolucao_pivot['melhorou'] == 0].shape[0]}")

#=============================TABELA 4: indicadores_rede=============================
print("\nCalculando indicadores por rede...\n")

ind_rede = (
    integrado
    .groupby(['ano', 'rede'])
    .agg(
        taxa_real_calculada=('in_alfabetizado', lambda x: round(x.mean() * 100, 2)),
        media_proficiencia_lp=('vl_proficiencia_lp', lambda x: round(x.mean(), 2)),
        total_alunos_avaliados=('id_aluno', 'count')
    )
    .reset_index()
)

print(f"  Total de registros: {ind_rede.shape[0]}")

#=============================SALVA NA GOLD=============================

print("\nSalvando tabelas na Gold...\n")

for ano in anos:
    salva_parquet_s3(
        ind_municipio[ind_municipio['ano'] == ano],
        f"gold/indicadores_municipio/year={ano}/dados.parquet"
    )
    salva_parquet_s3(
        ind_uf[ind_uf['ano'] == ano],
        f"gold/indicadores_uf/year={ano}/dados.parquet"
    )
    salva_parquet_s3(
        ind_rede[ind_rede['ano'] == ano],
        f"gold/indicadores_rede/year={ano}/dados.parquet"
    )

salva_parquet_s3(evolucao_pivot, "gold/evolucao_temporal/dados.parquet")

print("\n✓ Camada Gold concluída!")