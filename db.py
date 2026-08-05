"""
Inspeciona a view vi_rn1:
  1. Mostra a definição da view (SHOW CREATE VIEW)
  2. Pega um rn1 real da base
  3. Roda EXPLAIN na query que a API executa
  4. Mede o tempo real da query (5 execuções)

Uso:  python3 verifica_view.py
"""
import time

from functions.pesquisa_operadora import get_conn


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            # 1) Definição da view
            print("=== Definição da view vi_rn1 ===")
            cursor.execute("SHOW CREATE VIEW vi_rn1")
            row = cursor.fetchone()
            # A coluna pode vir como "Create View" dependendo da versão
            definicao = row.get("Create View") or list(row.values())[1]
            print(definicao)

            # 2) Pega um rn1 real para testar
            cursor.execute(
                "SELECT rn1 FROM stfc_cadup WHERE rn1 IS NOT NULL LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                print("\nNenhum rn1 encontrado em stfc_cadup para testar.")
                return
            rn1 = row["rn1"]
            print(f"\nUsando rn1 de teste: {rn1}")

            # 3) EXPLAIN da query da API
            print("\n=== EXPLAIN: vi_rn1 por rn1 ===")
            cursor.execute(
                "EXPLAIN SELECT prestadora FROM vi_rn1 WHERE rn1 = %s LIMIT 1",
                (rn1,),
            )
            for r in cursor.fetchall():
                print(
                    "  table={:<20} type={:<8} possible_keys={}  key={}  rows={}  extra={}".format(
                        str(r.get("table")),
                        str(r.get("type")),
                        r.get("possible_keys"),
                        r.get("key"),
                        r.get("rows"),
                        r.get("Extra"),
                    )
                )

            # 4) Tempo real da query
            print("\n=== Tempo real da query (5 execuções) ===")
            tempos = []
            for i in range(5):
                inicio = time.perf_counter()
                cursor.execute(
                    "SELECT prestadora FROM vi_rn1 WHERE rn1 = %s LIMIT 1",
                    (rn1,),
                )
                resultado = cursor.fetchone()
                ms = (time.perf_counter() - inicio) * 1000
                tempos.append(ms)
                print(f"  execucao {i + 1}: {ms:8.2f} ms  ->  {resultado}")

            print(f"\n  media: {sum(tempos) / len(tempos):.2f} ms")
            print(f"  minimo: {min(tempos):.2f} ms | maximo: {max(tempos):.2f} ms")

            if sum(tempos) / len(tempos) > 50:
                print(
                    "\n  >>> Query LENTA para um simples de-para. "
                    "A view provavelmente e o gargalo. <<<"
                )
            else:
                print("\n  Query rapida. A view nao e o gargalo.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()