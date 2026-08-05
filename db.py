"""
Verifica os índices das tabelas usadas na consulta de operadora e mostra
o plano de execução (EXPLAIN) das queries reais.

Uso:  python3 verifica_indexes.py
"""
from functions.pesquisa_operadora import get_conn

TABELAS = ["number_route_1", "stfc_cadup"]


def mostra_indexes(cursor, tabela):
    print(f"\n=== Índices de {tabela} ===")
    cursor.execute(f"SHOW INDEX FROM {tabela}")
    rows = cursor.fetchall()

    if not rows:
        print("  >>> NENHUM ÍNDICE ENCONTRADO! <<<")
        return

    for r in rows:
        unico = "UNIQUE" if r["Non_unique"] == 0 else "      "
        print(
            "  {} chave={:<20} coluna={:<15} posicao={} cardinalidade={}".format(
                unico,
                r["Key_name"],
                r["Column_name"],
                r["Seq_in_index"],
                r["Cardinality"],
            )
        )


def mostra_explain(cursor, descricao, sql, params):
    print(f"\n=== EXPLAIN: {descricao} ===")
    cursor.execute("EXPLAIN " + sql, params)
    for r in cursor.fetchall():
        print(
            "  type={:<8} possible_keys={}  key={}  rows={}  extra={}".format(
                str(r.get("type")),
                r.get("possible_keys"),
                r.get("key"),
                r.get("rows"),
                r.get("Extra"),
            )
        )


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            for tabela in TABELAS:
                mostra_indexes(cursor, tabela)

            # As mesmas queries que a API executa
            mostra_explain(
                cursor,
                "number_route_1 por tn",
                "SELECT rn1 FROM number_route_1 WHERE tn = %s LIMIT 1",
                ("11987069513",),
            )
            mostra_explain(
                cursor,
                "stfc_cadup por cn/prefixo/faixa",
                """SELECT rn1 FROM stfc_cadup
                   WHERE cn = %s AND prefixo = %s
                     AND %s >= faixa_inicial AND %s <= faixa_final
                   LIMIT 1""",
                ("11", "98706", "9513", "9513"),
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()