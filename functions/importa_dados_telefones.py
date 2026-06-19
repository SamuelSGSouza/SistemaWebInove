
import pymysql
from pymysql.cursors import SSDictCursor
import os
import pandas as pd
import datetime
from django.utils import timezone
from data.models import TelefonesDiscados
from functions.utils import clean_phone_number


def cadastra_telefones_dia():
    # --- Configuração de conexão (sobrescrevível por variável de ambiente) ---
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
        """Monta o SQL. Com limit > 0, pega os mais recentes (ORDER BY ... DESC LIMIT n).

        O LIMIT é interpolado direto (int validado), nunca o filtro de usuário.
        """
        base = """
            SELECT dst_clean, hangup_desc, calldate
            FROM chamadas
            WHERE calldate BETWEEN %(inicio)s AND %(fim)s
        """
        if limit > 0:
            return base + f"ORDER BY calldate DESC LIMIT {int(limit)}"
        return base + "ORDER BY calldate ASC"

    with conn.cursor() as cur:

        dia = str(datetime.datetime.today().day).zfill(2)
        mes = str(datetime.datetime.today().month).zfill(2)
        ano= datetime.datetime.today().year
        print(dia, mes, ano)

        cur.execute(montar_query(0), {"inicio": f"{ano}-{mes}-{dia} 00:00:00", "fim": f'{ano}-{mes}-{dia} 23:59:59'})
        linhas = list(cur.fetchall())
        df = pd.DataFrame(linhas)
        df["hangup_desc"] = df["hangup_desc"].apply(lambda x: True if "200 - OK" in str(x) else False)

        #todas as infos =  calldate, conta, src, dst_clean, duration, billsec, id_term, hangup_desc

        df.rename(columns={
            "dst_clean": "Telefone",
            "hangup_desc": "Sucesso Chamada",
            "calldate": "Momento Chamada"
        }, inplace=True)

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
