from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='despesas_pipeline', default_args=default_args, schedule=None, catchup=False)
def despesas_pipeline():
    
    @task()
    def get_max_id():
        with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COALESCE(MAX(id), 0) FROM despesas_raw')
                return cur.fetchone()[0]
    
    @task()
    def extract(max_id):
        response = requests.get(f"http://localhost:8080/despesa/listar/min/{max_id}")
        response.raise_for_status()
        return response.json()
    
    @task()
    def load(despesas):
        with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
            with conn.cursor() as cur:
                for despesa in despesas:
                    p = despesa.get("parcelamento")
                    cur.execute("""
                        INSERT INTO despesas_raw (
                            id, descricao, valor_original, valor_total,
                            tipo, prioridade, status, data_vencimento, data_criacao, data_atualizacao,
                            parc_id, parc_status, parc_valor_original, parc_valor_parcela, parc_valor_parcelado,
                            parc_quantidade, parc_parcelas_pagas, parc_data_inicio,
                            parc_tipo_juros, parc_taxa_juros
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        despesa.get("id"),
                        despesa.get("descricao"),
                        despesa.get("valorOriginal"),
                        despesa.get("valorTotal"),
                        despesa.get("tipo"),
                        despesa.get("prioridade"),
                        despesa.get("status"),
                        despesa.get("dataVencimento"),
                        despesa.get("dataCriacao"),
                        despesa.get("dataAtualizacao"),
                        p.get("id") if p else None,
                        p.get("parcelamentoStatus") if p else None,
                        p.get("valorOriginal") if p else None,
                        p.get("valorParcela") if p else None,
                        p.get("valorParcelado") if p else None,
                        p.get("quantidadeParcelas") if p else None,
                        p.get("parcelasPagas") if p else None,
                        p.get("dataInicio") if p else None,
                        p.get("tipoJuros") if p else None,
                        p.get("taxaJuros") if p else None,
                    ))
            conn.commit()

    max_id = get_max_id()
    despesas = extract(max_id)
    load(despesas)

despesas_pipeline_dag = despesas_pipeline()