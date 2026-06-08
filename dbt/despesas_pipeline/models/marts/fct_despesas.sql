{{ config(materialized='table') }}

SELECT
    id,
    descricao,
    tipo,
    prioridade,
    status,
    valor_original,
    valor_total,
    data_vencimento,
    data_criacao,
    data_atualizacao,
    parc_id

FROM {{ ref('stg_despesas') }}