{{ config(materialized='view') }}

SELECT
    id,
    descricao,
    prioridade,
    status,
    valor_original,
    valor_total,
    parc_id,
    parc_status,
    parc_quantidade,
    parc_valor_parcelado,
    parc_parcelas_pagas,
    parc_valor_parcela,
    parc_taxa_juros,
    parc_tipo_juros,
    parc_data_inicio,
    data_vencimento,

    parc_quantidade - parc_parcelas_pagas as parcelas_restantes,
    parc_valor_parcela * (parc_quantidade - parc_parcelas_pagas) as valor_restante

FROM {{ ref('stg_despesas') }} WHERE tipo = 'PARCELADA'