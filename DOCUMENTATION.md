# Documentação Técnica — Pipeline de Dados Educacionais

Este documento detalha o funcionamento de cada script do projeto, complementando o `README.md` com uma visão técnica aprofundada: funções, parâmetros, fluxo de dados e decisões de implementação.

---

## Índice

1. [Camada Bronze](#camada-bronze)
   - [`ingest_batch.py`](#ingest_batchpy)
   - [`kafka_producer.py`](#kafka_producerpy)
   - [`ingest_streaming.py`](#ingest_streamingpy)
   - [`main_streaming.py`](#main_streamingpy)
2. [Camada Silver](#camada-silver)
   - [`clean_transform.py`](#clean_transformpy)
   - [`integrate_sources.py`](#integrate_sourcespy)
3. [Camada Gold](#camada-gold)
   - [`build_indicators.py`](#build_indicatorspy)
4. [Validação](#validação)
   - [`check_duplicates.py`](#check_duplicatespy)

---

## Camada Bronze

### `ingest_batch.py`

**Propósito:** Ler os CSVs brutos armazenados em `data/raw/`, particioná-los por ano e enviá-los ao S3 em formato Parquet, seguindo o padrão de particionamento Hive.

**Fluxo de execução:**
```
1. Lê os 6 CSVs de data/raw/ com pandas
2. Para cada base, filtra os registros por ano (2023, 2024)
3. Converte cada subconjunto para Parquet
4. Faz upload para o S3 no caminho bronze/{nome_base}/year={ano}/dados.parquet
5. Remove o arquivo Parquet temporário local
```

**Função principal:**

```python
processa_base(df, col_ano, nome_df)
```
| Parâmetro | Tipo | Descrição |
|---|---|---|
| `df` | DataFrame | Base de dados já carregada em memória |
| `col_ano` | str | Nome da coluna que contém o ano de referência |
| `nome_df` | str | Nome da base — usado para montar o caminho no S3 |

**Bases processadas:** `uf`, `municipio`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`, `meta_alfabetizacao_brasil`, `alunos`

**Saída no S3:**
```
bronze/{base}/year={ano}/dados.parquet
```

---

### `kafka_producer.py`

**Propósito:** Simular um produtor Kafka gerando eventos fictícios de novas avaliações de alunos, para demonstrar o padrão de streaming da arquitetura.

**Fluxo de execução:**
```
1. Carrega a base de alunos para montar um dicionário UF → municípios válidos
2. Gera eventos aleatórios respeitando a relação geográfica
3. Coloca cada evento em uma fila compartilhada (queue.Queue)
4. Aguarda um intervalo entre eventos, simulando chegada em tempo real
```

**Funções principais:**

```python
gera_evento()
```
Gera um dicionário representando um evento de avaliação, com 8 campos: `NU_ANO_AVALIACAO`, `SG_UF`, `CO_MUNICIPIO`, `ID_ALUNO`, `ID_ESCOLA`, `VL_PROFICIENCIA_LP`, `IN_ALFABETIZADO`. O campo `IN_ALFABETIZADO` é derivado da proficiência (`>= 743` pontos), não sorteado — garante consistência interna do evento.

```python
produtor(n_eventos=20, intervalo_segundos=1)
```
| Parâmetro | Tipo | Descrição |
|---|---|---|
| `n_eventos` | int | Quantidade de eventos a gerar |
| `intervalo_segundos` | float | Pausa entre a geração de cada evento |

**Decisão de dados:** A UF Roraima (RR) foi excluída da simulação por não constar nos microdados do Saeb utilizados como referência para o dicionário UF→município.

---

### `ingest_streaming.py`

**Propósito:** Consumir os eventos gerados pelo produtor e persisti-los no S3, um arquivo Parquet por evento.

**Fluxo de execução:**
```
1. Consome eventos da fila compartilhada (fila_kafka) enquanto ela não estiver vazia
2. Converte cada evento em um DataFrame de uma linha
3. Salva como Parquet local com timestamp único no nome
4. Faz upload para o S3 em bronze/streaming/year={ano}/month={mes}/
5. Remove o arquivo temporário local
```

**Função principal:**

```python
processa_evento_streaming()
```
Não recebe parâmetros — consome diretamente da fila importada de `kafka_producer.py`. Usa `datetime.now()` para determinar a partição de destino e gerar um nome de arquivo único por evento (`evento_{timestamp}.parquet`), evitando sobrescritas quando múltiplos eventos chegam próximos no tempo.

**Saída no S3:**
```
bronze/streaming/year={ano}/month={mes}/evento_{timestamp}.parquet
```

---

### `main_streaming.py`

**Propósito:** Orquestrar a execução do produtor e do consumidor em conjunto, simulando o pipeline de streaming completo.

**Fluxo de execução:**
```
1. Inicia o produtor em uma thread separada
2. Aguarda a thread do produtor finalizar (join)
3. Executa o consumidor, processando todos os eventos acumulados na fila
```

**Função principal:**

```python
main()
```
Usa `threading.Thread` para simular a natureza concorrente de produtores e consumidores em um sistema Kafka real, ainda que nesta simulação a execução seja sequencial (produtor termina antes do consumidor iniciar).

---

## Camada Silver

### `clean_transform.py`

**Propósito:** Ler os dados brutos da Bronze, aplicar regras de limpeza e padronização, e salvar o resultado tratado na Silver.

**Fluxo de execução:**
```
1. Lê todas as bases da Bronze (concatenando os anos 2023 e 2024)
2. Aplica tratamentos específicos por base
3. Remove duplicatas de todas as bases
4. Salva cada base tratada na Silver, particionada por ano
```

**Funções utilitárias:**

```python
le_parquet_s3(caminho_s3)
```
Lê um arquivo Parquet diretamente do S3 para memória, sem necessidade de download para disco — usa `boto3` + `BytesIO`.

```python
salva_parquet_s3(df, caminho_s3)
```
Salva um DataFrame como Parquet temporário local, faz upload para o S3 e remove o arquivo local em seguida.

```python
dropa_duplicadas(df)
```
Remove linhas duplicadas de um DataFrame e reporta quantas foram removidas.

**Tratamentos aplicados por base:**

| Base | Tratamento |
|---|---|
| **Alunos** | Renomeação de colunas do padrão INEP para o padrão Base dos Dados; conversão de `rede` (inteiro → texto); remoção de rede Privada; remoção de alunos ausentes (`IN_PRESENCA_LP = 0`); correção de tipo de `IN_ALFABETIZADO` |
| **UF** | Remoção de registros com `rede = 0` (inválido); conversão de `rede` (inteiro → texto, incluindo `5 = 'Pública'`) |
| **Município** | Mesmos tratamentos da base UF |
| **Meta Município / Meta Brasil** | Padronização de tipo da coluna `meta_alfabetizacao_2030` (int → float) |

**Mapeamentos de `rede` utilizados:**
```python
# Base alunos (dicionário oficial INEP)
{1: 'Federal', 2: 'Estadual', 3: 'Municipal', 4: 'Privada'}

# Bases UF e Município (dicionário oficial Base dos Dados)
{2: 'Estadual', 3: 'Municipal', 5: 'Pública'}
```

---

### `integrate_sources.py`

**Propósito:** Integrar a base de alunos com os dados agregados de município, criando a tabela `silver/integrado/` que serve de base para os cálculos da Gold.

**Fluxo de execução:**
```
1. Lê as bases alunos e municipio da Silver
2. Padroniza as colunas de alunos para minúsculo
3. Seleciona apenas colunas relevantes de municipio (evita conflito de nomes)
4. Executa o join alunos + municipio pela chave (ano, id_municipio, rede)
5. Valida que o número de linhas não mudou após o join (assert)
6. Salva o resultado particionado por ano
```

**Chave de integração:** `ano + id_municipio + rede`

**Decisão de dados:** As colunas de meta (município e UF) não são incluídas nesta integração, permanecem nas suas tabelas originais na Silver e são unidas apenas na camada Gold, após a agregação por município. Essa decisão evita repetir os mesmos valores de meta para cada um dos 3,3 milhões de registros de alunos, economizando memória e espaço em disco.

**Colunas adicionadas à base de alunos:**
```
mun_taxa_alfabetizacao   (prefixo mun_ evita conflito com indicador calculado na Gold)
mun_media_portugues
```

---

## Camada Gold

### `build_indicators.py`

**Propósito:** Calcular os indicadores analíticos finais a partir da tabela integrada da Silver, gerando 4 tabelas prontas para consumo.

**Fluxo de execução:**
```
1. Lê silver/integrado/, meta_municipio, meta_uf e uf
2. Calcula indicadores_municipio (agregação por ano + id_municipio + rede)
3. Calcula indicadores_uf (agregação por ano + sigla_uf + rede)
4. Calcula evolucao_temporal (pivot comparando 2023 vs 2024)
5. Calcula indicadores_rede (agregação por ano + rede)
6. Salva as 4 tabelas na Gold
```

**Tabela `indicadores_municipio`:**

| Coluna | Cálculo |
|---|---|
| `taxa_real_calculada` | `mean(in_alfabetizado) * 100`, agregado por município |
| `media_proficiencia_lp` | `mean(vl_proficiencia_lp)` |
| `gap_meta_2024` | `taxa_real_calculada - meta_mun_alf_2024` |
| `flag_risco` | `1` se `taxa_real_calculada < 50`, senão `0` |
| `crescimento_anual_necessario` | `(meta_mun_alf_2030 - taxa_real_calculada) / anos_restantes` |

**Tabela `indicadores_uf`:**

Inclui um tratamento especial: como a base `meta_uf` define metas apenas para `rede = 'Pública'` (agregado de Estadual + Municipal), o script calcula esse agregado a partir dos microdados antes de fazer o join com as metas:

```python
ind_uf_publica = integrado[integrado['rede'].isin(['Estadual', 'Municipal'])]
    .groupby(['ano', 'sigla_uf'])
    .agg(...)
ind_uf_publica['rede'] = 'Pública'
```

Também calcula `desigualdade_interna`, a diferença entre a maior e a menor taxa de alfabetização entre os municípios de uma mesma UF.

**Tabela `evolucao_temporal`:**

Usa `pivot_table` para colocar as taxas de 2023 e 2024 lado a lado por município, permitindo calcular:
- `variacao`: diferença entre os dois anos
- `atingiu_meta_2023` / `atingiu_meta_2024`: flags binárias
- `melhorou`: flag indicando se houve progresso

**Tabela `indicadores_rede`:**

Agregação simples por `ano + rede`, comparando taxa de alfabetização e proficiência entre redes Estadual e Municipal.

---

## Validação

### `check_duplicates.py`

**Propósito:** Validar a qualidade dos dados na camada Silver, verificando duplicatas totais e duplicatas de chave primária em todas as bases.

**Função principal:**

```python
verifica_duplicatas(df, nome, chave_primaria)
```
| Parâmetro | Tipo | Descrição |
|---|---|---|
| `df` | DataFrame | Base a ser validada |
| `nome` | str | Nome da base, usado no relatório impresso |
| `chave_primaria` | list[str] | Colunas que compõem a chave única esperada |

**Duas verificações realizadas:**

1. **Duplicata total** — linhas inteiramente iguais (`df.duplicated()`)
2. **Duplicata de chave** — mesma chave primária aparecendo mais de uma vez, indicando possível conflito de dados (`df.duplicated(subset=chave_primaria)`)

**Chaves primárias validadas:**

| Base | Chave |
|---|---|
| Alunos | `id_aluno + ano` |
| UF | `sigla_uf + rede + ano` |
| Município | `id_municipio + rede + ano` |
| Meta Brasil | `rede + ano` |
| Meta UF | `sigla_uf + rede + ano` |
| Meta Município | `id_municipio + rede + ano` |

---
