
import pymysql
from pymysql.cursors import SSDictCursor
import os
import pandas as pd
import datetime
from django.utils import timezone
from data.models import TelefonesDiscados
from functions.utils import clean_phone_number


def cadastra_telefones_dia():
    # --- Configuração de conexão com o banco de origem (MySQL das chamadas) ---
    DB_HOST = "177.39.236.251"
    DB_PORT = int("3306")
    DB_USER = "inove_db2"
    DB_PASSWORD = "4g2dH4cmyzcLUswTIc3z0cVXj"
    DB_NAME = "brdsoft"

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

    def montar_query(limit: int) -> str:
        base = """
            SELECT dst_clean, hangup_desc, calldate
            FROM chamadas
            WHERE calldate BETWEEN %(inicio)s AND %(fim)s
        """
        if limit > 0:
            return base + f"ORDER BY calldate DESC LIMIT {int(limit)}"
        return base + "ORDER BY calldate ASC"

    try:
        with conn.cursor() as cur:
            hoje = datetime.date.today()
            inicio = f"{hoje:%Y-%m-%d} 00:00:00"
            fim = f"{hoje:%Y-%m-%d} 23:59:59"

            cur.execute(montar_query(0), {"inicio": inicio, "fim": fim})
            linhas = list(cur.fetchall())
    finally:
        conn.close()

    if not linhas:
        return  # nada discado hoje

    df = pd.DataFrame(linhas)

    df["sucesso_chamada"] = df["hangup_desc"].apply(
        lambda x: "200 - OK" in str(x)
    )
    df["telefone"] = df["dst_clean"].apply(clean_phone_number)
    df.rename(columns={"calldate": "momento_chamada"}, inplace=True)

    # Deduplica dentro do próprio dia:
    # prioriza sucesso=True e, no empate, o momento mais recente
    df = (
        df.sort_values(
            ["sucesso_chamada", "momento_chamada"],
            ascending=[False, False],
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

    # Sucessos: cria se o telefone não existe; se existe,
    # atualiza só o bool e o momento (upsert via ON CONFLICT DO UPDATE)
    if objs_sucesso:
        TelefonesDiscados.objects.bulk_create(
            objs_sucesso,
            update_conflicts=True,
            update_fields=["sucesso_chamada", "momento_chamada"],
            unique_fields=["telefone"],
        )

    # Falhas: só insere se o telefone ainda não estiver cadastrado
    # (ON CONFLICT DO NOTHING)
    if objs_falha:
        TelefonesDiscados.objects.bulk_create(
            objs_falha,
            ignore_conflicts=True,
        )

    # Limpeza de registros antigos (90 dias)
    limite = timezone.now() - datetime.timedelta(days=90)
    TelefonesDiscados.objects.filter(momento_chamada__lt=limite).delete()
    
def cadastra_telefones_antigos():
    dir_telefones = os.path.join(os.getcwd(), "Telefones_Chamados")
    for file in os.listdir(dir_telefones):
        filepath = os.path.join(dir_telefones, file)
        df = pd.read_csv(filepath, sep=";", dtype=str)

        df["Telefone"] = df["Telefone"].apply(lambda x: clean_phone_number(x))
        
        objs = []
        for index, row in df.iterrows():
            dt = datetime.datetime.strptime(row["Momento Chamada"], "%Y-%m-%d %H:%M:%S")
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt)
            
            objs.append(TelefonesDiscados(
                telefone=row["Telefone"],
                sucesso_chamada=row["Sucesso Chamada"].strip().lower() == "true",
                momento_chamada=dt,
            ))
        
        TelefonesDiscados.objects.bulk_create(objs)

        print(f"Arquivo {file} Cadastrado!")
