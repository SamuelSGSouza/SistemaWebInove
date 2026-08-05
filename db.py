"""
Inspeciona a view vi_rn1 (tolerante a falta de privilegios).

  1. Tenta mostrar a definicao da view (requer privilegio SHOW VIEW - opcional)
  2. Mostra as colunas da view (DESCRIBE)
  3. Pega um rn1 real da base
  4. Roda EXPLAIN na query que a API executa
  5. Mede o tempo real da query (5 execucoes)
  6. Mede o tempo de carregar a view INTEIRA (para avaliar cache em memoria)

Uso:  python3 verifica_view.py
"""
import time

import pymysql

from functions.pesquisa_operadora import get_conn


def secao(titulo):
    print(f"\n=== {titulo} ===")


def main():
    conn = get_conn()
    try:
        with conn.cursor() as cursor:

            # 1) Definicao da view (opcional - pode faltar privilegio)
            secao("Definicao da view vi_rn1")
            try:
                cursor.execute("SHOW CREATE VIEW vi_rn1")
                row = cursor.fetchone()
                definicao = row.get("Create View") or list(row.values())[1]
                print(definicao)
            except pymysql.err.OperationalError as e:
                print(f"  [pulado] Sem privilegio SHOW VIEW: {e.args[1]}")
                print("  (nao impede o diagnostico - seguindo em frente)")

            # 2) Colunas da view
            secao("Colunas da view")
            try:
                cursor.execute("DESCRIBE vi_rn1")
                for r in cursor.fetchall():
                    print("  {:<20} {}".format(str(r.get("Field")), r.get("Type")))
            except Exception as e:
                print(f"  [pulado] {e}")

            # 3) rn1 real para teste
            cursor.execute(
                "SELECT rn1 FROM stfc_cadup WHERE rn1 IS NOT NULL LIMIT 1"
            )
            row = cursor.fetchone()
            if not row:
                print("\nNenhum rn1 encontrado em stfc_cadup para testar.")
                return
            rn1 = row["rn1"]
            print(f"\nUsando rn1 de teste: {rn1}")

            # 4) EXPLAIN da query da API
            secao("EXPLAIN: vi_rn1 por rn1")
            try:
                cursor.execute(
                    "EXPLAIN SELECT prestadora FROM vi_rn1 WHERE rn1 = %s LIMIT 1",
                    (rn1,),
                )
                for r in cursor.fetchall():
                    print(
                        "  table={:<22} type={:<8} possible_keys={}  key={}  rows={}  extra={}".format(
                            str(r.get("table")),
                            str(r.get("type")),
                            r.get("possible_keys"),
                            r.get("key"),
                            r.get("rows"),
                            r.get("Extra"),
                        )
                    )
            except Exception as e:
                print(f"  [pulado] {e}")

            # 5) Tempo real da query pontual
            secao("Tempo real da query (5 execucoes)")
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

            media = sum(tempos) / len(tempos)
            print(f"\n  media: {media:.2f} ms | min: {min(tempos):.2f} | max: {max(tempos):.2f}")

            if media > 50:
                print("\n  >>> LENTA para um de-para simples. A view e o gargalo. <<<")
            else:
                print("\n  Query rapida. A view NAO e o gargalo.")

            # 6) Carregar a view inteira (viabilidade de cache em memoria)
            secao("Carregar a view INTEIRA (avaliar cache em memoria)")
            inicio = time.perf_counter()
            cursor.execute("SELECT rn1, prestadora FROM vi_rn1")
            linhas = cursor.fetchall()
            ms = (time.perf_counter() - inicio) * 1000
            print(f"  {len(linhas)} linhas carregadas em {ms:.2f} ms")
            print("  amostra:", linhas[:3])
            if len(linhas) < 50000:
                print("\n  Volume pequeno: cache em memoria e viavel e elimina essa query.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()