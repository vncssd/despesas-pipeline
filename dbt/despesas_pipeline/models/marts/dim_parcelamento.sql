{{ config(materialized='table') }}

SELECT
    descricao,
    parc_id,
    parc_status,
    parc_valor_parcela,
    parc_valor_parcelado,
    parc_quantidade,
    parc_parcelas_pagas,
    parc_data_inicio,
    parc_tipo_juros,
    parc_taxa_juros,
    parcelas_restantes,
    valor_restante

FROM {{ ref('int_despesas_parceladas') }}