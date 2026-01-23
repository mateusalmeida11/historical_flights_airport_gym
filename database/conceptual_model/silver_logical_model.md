# Logical Data Model – Flights Analytics

Este documento descreve o **modelo lógico de dados** utilizado na camada Gold
do projeto de análise de voos no Brasil, construído a partir de dados da API da ANAC.

O modelo segue o padrão **Star Schema**, conforme as boas práticas do
Kimball Group, com foco em análises analíticas e performance de consulta.

---

## Grain Definition

**Grão da tabela fato:**
Cada registro da tabela fato representa a **execução de um voo específico**,
realizado por uma única companhia aérea, utilizando uma única aeronave,
entre um aeroporto de origem e um aeroporto de destino, em uma data e horário definidos.

---

## Fact Table

### `fact_voos`

Tabela central do modelo, contendo métricas relacionadas à execução dos voos.

**Medidas principais:**
- `numero_assentos_ofertados`
- `atraso_minutos_partida`
- `atraso_minutos_chegada`

**Chave Primária:**
- `voo_id`

**Chaves estrangeiras:**
- Companhia aérea
- Aeronave
- Aeroporto de origem
- Aeroporto de destino
- Datas e horários (previstos e reais)
- Informações de status do voo

---

## Dimensions

### `dim_companhias_aereas`
Armazena informações institucionais das companhias aéreas.

### `dim_aeronaves`
Contém características técnicas das aeronaves utilizadas nos voos.

### `dim_aeroportos`
Representa aeroportos de origem e destino (role-playing dimension).

### `dim_date`
Dimensão de data utilizada em múltiplos contextos:
- partida prevista
- partida real
- chegada prevista
- chegada real

### `dim_tempo`
Dimensão de tempo (hora, minuto, segundo), utilizada como role-playing dimension.

### `dim_infos_voo`
Junk dimension contendo atributos operacionais do voo, como situação e motivo.

---

## Relationships and Cardinality

- Voos → Companhias Aéreas: **N:1**
- Voos → Aeronaves: **N:1**
- Voos → Aeroportos (Origem): **N:1**
- Voos → Aeroportos (Destino): **N:1**
- Voos → Datas: **N:1** (role-playing)
- Voos → Tempo: **N:1** (role-playing)
- Voos → Informações do Voo: **N:1**

---

## Design Decisions

- O modelo foi projetado para análise da **execução de voos**, não de rotas.
- Relações muitos-para-muitos foram evitadas através da definição clara do grão.
- Datas e horários previstos e reais foram mantidos para permitir análises
  comparativas entre planejamento e execução.
- Atributos de baixa cardinalidade e natureza categórica foram agrupados
  em uma junk dimension.
