"""
Cronometra CADA query do fluxo separadamente, para achar qual delas
consome os ~400ms.

Uso:  python3 mede_queries.py
"""
import time

from functions.pesquisa_operadora import get_conn

# Um portado (achado em number_route_1) e um nao portado (cai no cadup)
TELEFONES = ["11987069513", "11999999999", "2133334444"]
N = 3


def cronometra(cursor, rotulo, sql, params, n=N):
    tempos = []
    resultado = None
    for _ in range(n):
        inicio = time.perf_counter()
        cursor.execute(sql, params)
        resultado = cursor.fetchone()
        tempos.append((time.perf_counter() - inicio) * 1000)
    media = sum(tempos) / len(tempos)
    marca = "  <<< LENTA" if media > 50 else ""
    print(f"  {rotulo:<38} {media:8.2f} ms  (min {min(tempos):.2f})"
          f"  -> {resultado}{marca}")
    return resultado


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            for telefone in TELEFONES:
                numero = "".join(c for c in telefone if c.isdigit())
                cn = numero[:2]
                corpo = numero[2:]
                prefixo = corpo[:-4]
                sufixo = corpo[-4:]

                print(f"\n=== Telefone {numero} ===")

                # Query 1: portados
                row = cronometra(
                    cursor,
                    "1) number_route_1 (tn)",
                    "SELECT rn1 FROM number_route_1 WHERE tn = %s LIMIT 1",
                    (numero,),
                )

                # Query 2: nao portados (roda sempre aqui, so para medir)
                row2 = cronometra(
                    cursor,
                    "2) stfc_cadup (cn/prefixo/faixa)",
                    """SELECT rn1 FROM stfc_cadup
                       WHERE cn = %s AND prefixo = %s
                         AND %s >= faixa_inicial AND %s <= faixa_final
                       LIMIT 1""",
                    (cn, prefixo, sufixo, sufixo),
                )

                # Query 3: prestadora
                rn1 = (row or row2 or {}).get("rn1")
                if rn1:
                    cronometra(
                        cursor,
                        "3) vi_rn1 (prestadora)",
                        "SELECT prestadora FROM vi_rn1 WHERE rn1 = %s LIMIT 1",
                        (rn1,),
                    )
                else:
                    print("  3) vi_rn1 .............................. [sem rn1]")

            # Tipos das colunas envolvidas — descasamento de tipo mata indice
            print("\n=== Tipos das colunas de busca ===")
            for tabela, colunas in [
                ("number_route_1", ("tn",)),
                ("stfc_cadup", ("cn", "prefixo", "faixa_inicial", "faixa_final")),
            ]:
                cursor.execute(f"DESCRIBE {tabela}")
                for r in cursor.fetchall():
                    if r["Field"] in colunas:
                        print(f"  {tabela}.{r['Field']:<16} {r['Type']}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()