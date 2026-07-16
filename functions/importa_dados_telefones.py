
import pymysql
from pymysql.cursors import SSDictCursor
import os
import pandas as pd
import datetime
from django.utils import timezone
from data.models import TelefonesDiscados
from functions.utils import clean_phone_number
from data.models import salva_log
import traceback

def cadastra_telefones_dia():
    """
    Cadastra os telefones discados nos últimos 90 dias, processando
    UM DIA POR VEZ, da data mais antiga para a mais recente.

    Regras:
    - Dentro de cada dia: prioriza sucesso (atendido) e, no empate,
        mantém o registro mais antigo do dia (a data original).
    - Entre dias: como vamos do mais antigo ao mais recente, o telefone
        é cadastrado com a data original (primeira ocorrência). Se depois
        aparecer uma chamada com sucesso para um telefone já cadastrado,
        atualiza APENAS o bool `sucesso_chamada` — o `momento_chamada`
        original é preservado, para que o registro expire naturalmente
        na limpeza de 90 dias.
    - Falhas em telefones já cadastrados são ignoradas (DO NOTHING).
    """
    # --- Configuração de conexão com o banco de origem (MySQL das chamadas) ---
    DB_HOST = "177.39.236.251"
    DB_PORT = int("3306")
    DB_USER = "inove_db2"
    DB_PASSWORD = "4g2dH4cmyzcLUswTIc3z0cVXj"
    DB_NAME = "brdsoft"
    
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            cursorclass=SSDictCursor,
            connect_timeout=10000,
            read_timeout=12000,
        )
    
        QUERY = """
            SELECT dst_clean, hangup_desc, calldate
            FROM chamadas
            WHERE calldate BETWEEN %(inicio)s AND %(fim)s
            ORDER BY calldate ASC
        """
    
        hoje = datetime.date.today()
        dia = hoje - datetime.timedelta(days=1)  # começa no dia mais antigo
    
        try:
            with conn.cursor() as cur:
                # Itera dia a dia: mais antigo -> mais recente
                while dia <= hoje:
                    inicio = f"{dia:%Y-%m-%d} 00:00:00"
                    fim = f"{dia:%Y-%m-%d} 23:59:59"
    
                    cur.execute(QUERY, {"inicio": inicio, "fim": fim})
                    linhas = list(cur.fetchall())
    
                    if linhas:
                        _processa_dia(linhas)
    
                    dia += datetime.timedelta(days=1)
        except Exception as e:
            salva_log(f"Erro INTERNO: {traceback.format_exc()}", sistema="TelefonesDiscados") 
        finally:
            
            conn.close()
    
        # Limpeza de registros antigos (90 dias)
        limite = timezone.now() - datetime.timedelta(days=90)
        TelefonesDiscados.objects.filter(momento_chamada__lt=limite).delete()
        
    except Exception as e:
        salva_log(f"Erro externo: {traceback.format_exc()}", sistema="TelefonesDiscados") 

    
def cadastra_telefones_antigos():
    """
    Cadastra os telefones discados nos últimos 90 dias, processando
    UM DIA POR VEZ, da data mais antiga para a mais recente.
 
    Regras:
    - Dentro de cada dia: prioriza sucesso (atendido) e, no empate,
      mantém o registro mais antigo do dia (a data original).
    - Entre dias: como vamos do mais antigo ao mais recente, o telefone
      é cadastrado com a data original (primeira ocorrência). Se depois
      aparecer uma chamada com sucesso para um telefone já cadastrado,
      atualiza APENAS o bool `sucesso_chamada` — o `momento_chamada`
      original é preservado, para que o registro expire naturalmente
      na limpeza de 90 dias.
    - Falhas em telefones já cadastrados são ignoradas (DO NOTHING).
    """
    # --- Configuração de conexão com o banco de origem (MySQL das chamadas) ---
    DB_HOST = "177.39.236.251"
    DB_PORT = int("3306")
    DB_USER = "inove_db2"
    DB_PASSWORD = "4g2dH4cmyzcLUswTIc3z0cVXj"
    DB_NAME = "brdsoft"
    
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset="utf8mb4",
            cursorclass=SSDictCursor,
            connect_timeout=10000,
            read_timeout=12000,
        )
    
        QUERY = """
            SELECT dst_clean, hangup_desc, calldate
            FROM chamadas
            WHERE calldate BETWEEN %(inicio)s AND %(fim)s
            ORDER BY calldate ASC
        """
    
        hoje = datetime.date.today()
        dia = hoje - datetime.timedelta(days=90)  # começa no dia mais antigo
    
        try:
            with conn.cursor() as cur:
                # Itera dia a dia: mais antigo -> mais recente
                while dia <= hoje:
                    inicio = f"{dia:%Y-%m-%d} 00:00:00"
                    fim = f"{dia:%Y-%m-%d} 23:59:59"
    
                    cur.execute(QUERY, {"inicio": inicio, "fim": fim})
                    linhas = list(cur.fetchall())
    
                    if linhas:
                        _processa_dia(linhas)
    
                    dia += datetime.timedelta(days=1)
        except Exception as e:
            salva_log(f"Erro INTERNO: {traceback.format_exc()}", sistema="TelefonesDiscados") 
        finally:
            
            conn.close()
    
        # Limpeza de registros antigos (90 dias)
        limite = timezone.now() - datetime.timedelta(days=90)
        TelefonesDiscados.objects.filter(momento_chamada__lt=limite).delete()
        
    except Exception as e:
        salva_log(f"Erro externo: {traceback.format_exc()}", sistema="TelefonesDiscados") 

def _processa_dia(linhas):
    """Deduplica e persiste as chamadas de um único dia."""
    df = pd.DataFrame(linhas)
 
    df["sucesso_chamada"] = df["hangup_desc"].apply(
        lambda x: "200 - OK" in str(x)
    )
    df["telefone"] = df["dst_clean"].apply(clean_phone_number)
    df.rename(columns={"calldate": "momento_chamada"}, inplace=True)
 
    # Deduplica dentro do dia:
    # prioriza sucesso=True e, no empate, o momento MAIS ANTIGO
    # (mantém a data original do telefone)
    df = (
        df.sort_values(
            ["sucesso_chamada", "momento_chamada"],
            ascending=[False, True],
        )
        .drop_duplicates(subset="telefone", keep="first")
    )
 
    objs_sucesso = []
    objs_falha = []
 
    for row in df.itertuples(index=False):
        dt = row.momento_chamada
        if isinstance(dt, str):
            dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt)
 
        obj = TelefonesDiscados(
            telefone=row.telefone,
            sucesso_chamada=bool(row.sucesso_chamada),
            momento_chamada=dt,
        )
        (objs_sucesso if obj.sucesso_chamada else objs_falha).append(obj)
 
    # Sucessos: cria se não existe; se já existe, atualiza SOMENTE o bool.
    # O momento_chamada NÃO entra em update_fields, garantindo que a
    # data original seja preservada.
    if objs_sucesso:
        TelefonesDiscados.objects.bulk_create(
            objs_sucesso,
            update_conflicts=True,
            update_fields=["sucesso_chamada"],
            unique_fields=["telefone"],
        )
 
    # Falhas: só insere se o telefone ainda não estiver cadastrado
    # (ON CONFLICT DO NOTHING) — preserva data e status existentes
    if objs_falha:
        TelefonesDiscados.objects.bulk_create(
            objs_falha,
            ignore_conflicts=True,
        )