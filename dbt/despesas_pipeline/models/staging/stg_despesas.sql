{{ config(materialized='view') }}

select
    id,
    descricao,
    valor_original,
    valor_total,
    tipo,
    prioridade,
    status,
    data_vencimento,
    data_criacao,
    data_atualizacao,
    parc_id,
    parc_status,
    parc_valor_original,
    parc_valor_parcela,
    parc_valor_parcelado,
    parc_quantidade,
    parc_parcelas_pagas,
    parc_data_inicio,
    parc_tipo_juros,
    parc_taxa_juros

from {{ source('despesas', 'DESPESAS_RAW') }}