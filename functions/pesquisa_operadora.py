import threading
from typing import Optional

import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
import os
load_dotenv()

EXTERNAL_2_DB_HOST = os.getenv("EXTERNAL_2_DB_HOST", "")
EXTERNAL_2_DB_PORT = int(os.getenv("EXTERNAL_2_DB_PORT", "3306"))
EXTERNAL_2_DB_USER = os.getenv("EXTERNAL_2_DB_USER", "")
EXTERNAL_2_DB_PASSWORD = os.getenv("EXTERNAL_2_DB_PASSWORD", "")
EXTERNAL_2_DB_NAME = os.getenv("EXTERNAL_2_DB_NAME", "")


def get_conn():
    """Abre uma conexão NOVA com o banco (caller é responsável por fechar)."""
    return pymysql.connect(
        host=EXTERNAL_2_DB_HOST,
        port=EXTERNAL_2_DB_PORT,
        user=EXTERNAL_2_DB_USER,
        password=EXTERNAL_2_DB_PASSWORD,
        database=EXTERNAL_2_DB_NAME,
        charset="utf8mb4",
        cursorclass=DictCursor,   # bufferizado: correto para queries LIMIT 1
        connect_timeout=10,
        read_timeout=30,
    )


# ---------------------------------------------------------------------------
# Conexão persistente por thread (para uso nas views / Gunicorn com threads)
# ---------------------------------------------------------------------------
_local = threading.local()


def get_conn_persistente():
    """
    Retorna uma conexão persistente exclusiva da thread atual.

    - Cada thread do Gunicorn mantém a própria conexão viva entre requisições,
      eliminando o custo de abrir/fechar conexão a cada chamada.
    - ping(reconnect=True) reconecta automaticamente se o MySQL derrubou a
      conexão por inatividade (wait_timeout).
    - NUNCA feche esta conexão manualmente.
    """
    conn = getattr(_local, "conn", None)
    if conn is None:
        _local.conn = get_conn()
    else:
        try:
            conn.ping(reconnect=True)
        except Exception:
            # Conexão irrecuperável: descarta e abre outra
            try:
                conn.close()
            except Exception:
                pass
            _local.conn = get_conn()
    return _local.conn


def _normaliza_telefone(telefone: str) -> str:
    """Remove tudo que não for dígito e o código do país (55), se houver."""
    numero = "".join(c for c in str(telefone) if c.isdigit())

    # Remove +55 se vier com código do país (ex.: 5511987069513)
    if len(numero) in (12, 13) and numero.startswith("55"):
        numero = numero[2:]

    if len(numero) not in (10, 11):
        raise ValueError(
            f"Telefone inválido: '{telefone}'. Esperado DDD + número (10 ou 11 dígitos)."
        )
    return numero


def _busca_prestadora(cursor, rn1) -> Optional[str]:
    """Busca o nome da prestadora na view vi_rn1 pelo rn1."""
    cursor.execute("SELECT prestadora FROM vi_rn1 WHERE rn1 = %s LIMIT 1", (rn1,))
    row = cursor.fetchone()
    return row["prestadora"] if row else None


def consulta_operadora(telefone: str, conn=None) -> dict:
    """
    Recebe um telefone (com DDD) e devolve a operadora.

    Fluxo:
      1. Consulta number_route_1 (números portados) pela coluna tn = ddd+numero.
      2. Se não encontrar, consulta stfc_cadup (não portados) por cn, prefixo e
         faixa_inicial/faixa_final.
      3. Com o rn1 encontrado, busca a prestadora na view vi_rn1.

    Se `conn` for passada, ela é reutilizada e NÃO é fechada aqui.
    Se não for passada, abre e fecha uma conexão própria.

    Retorna:
      {
        "telefone": "11987069513",
        "portado": True/False,
        "rn1": ...,
        "operadora": "TIM" | "VIVO" | ... | None,
      }
    """
    numero = _normaliza_telefone(telefone)

    cn = numero[:2]            # DDD
    corpo = numero[2:]         # número sem DDD (8 ou 9 dígitos)
    prefixo = corpo[:-4]       # tudo menos os 4 últimos dígitos
    sufixo = corpo[-4:]        # 4 últimos dígitos (comparado com as faixas)

    fechar_conn = False
    if conn is None:
        conn = get_conn()
        fechar_conn = True

    try:
        with conn.cursor() as cursor:
            # 1) Números portados — sempre consultar primeiro
            cursor.execute(
                "SELECT rn1 FROM number_route_1 WHERE tn = %s LIMIT 1", (numero,)
            )
            row = cursor.fetchone()

            if row:
                rn1 = row["rn1"]
                return {
                    "telefone": numero,
                    "portado": True,
                    "rn1": rn1,
                    "operadora": _busca_prestadora(cursor, rn1),
                }

            # 2) Não portados (CADUP)
            cursor.execute(
                """
                SELECT rn1
                  FROM stfc_cadup
                 WHERE cn = %s
                   AND prefixo = %s
                   AND %s >= faixa_inicial
                   AND %s <= faixa_final
                 LIMIT 1
                """,
                (cn, prefixo, sufixo, sufixo),
            )
            row = cursor.fetchone()

            if row:
                rn1 = row["rn1"]
                return {
                    "telefone": numero,
                    "portado": False,
                    "rn1": rn1,
                    "operadora": _busca_prestadora(cursor, rn1),
                }

            # Não encontrado em nenhuma das tabelas
            return {
                "telefone": numero,
                "portado": None,
                "rn1": None,
                "operadora": None,
            }
    finally:
        if fechar_conn:
            conn.close()


def consulta_operadora_lote(telefones, conn=None):
    """
    Consulta uma lista de telefones reutilizando uma única conexão.

    Recebe uma lista/iterável de telefones e retorna uma lista de dicts no
    mesmo formato de consulta_operadora(). Telefones inválidos não interrompem
    o lote: entram no resultado com o campo "erro" preenchido.
    """
    fechar_conn = False
    if conn is None:
        conn = get_conn()
        fechar_conn = True

    resultados = []
    try:
        for tel in telefones:
            try:
                resultados.append(consulta_operadora(tel, conn=conn))
            except ValueError as e:
                resultados.append({
                    "telefone": str(tel),
                    "portado": None,
                    "rn1": None,
                    "operadora": None,
                    "erro": str(e),
                })
    finally:
        if fechar_conn:
            conn.close()

    return resultados


def consulta_operadora_arquivo(caminho_entrada, caminho_saida=None):
    """
    Lê um arquivo texto com um telefone por linha, consulta todos em lote e,
    opcionalmente, grava o resultado em um CSV (telefone;portado;rn1;operadora;erro).

    Retorna a lista de resultados.
    """
    with open(caminho_entrada, "r", encoding="utf-8") as f:
        telefones = [linha.strip() for linha in f if linha.strip()]

    resultados = consulta_operadora_lote(telefones)

    if caminho_saida:
        with open(caminho_saida, "w", encoding="utf-8") as f:
            f.write("telefone;portado;rn1;operadora;erro\n")
            for r in resultados:
                f.write("{};{};{};{};{}\n".format(
                    r.get("telefone", ""),
                    r.get("portado", ""),
                    r.get("rn1", ""),
                    r.get("operadora", ""),
                    r.get("erro", ""),
                ))

    return resultados


if __name__ == "__main__":
    # Consulta individual
    resultado = consulta_operadora("11987069513")
    print(f"Telefone : {resultado['telefone']}")
    print(f"Portado  : {resultado['portado']}")
    print(f"RN1      : {resultado['rn1']}")
    print(f"Operadora: {resultado['operadora']}")

    print("-" * 40)

    # Consulta em lote (lista)
    lote = ["11987069513", "(21) 99876-5432", "abc123"]
    for r in consulta_operadora_lote(lote):
        if r.get("erro"):
            print(f"{r['telefone']}: ERRO - {r['erro']}")
        else:
            print(f"{r['telefone']}: {r['operadora']} (portado={r['portado']})")

    # Consulta em lote a partir de arquivo, gravando CSV:
    # consulta_operadora_arquivo("telefones.txt", "resultado.csv")