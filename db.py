
"""
Separa o tempo de ABRIR CONEXAO do tempo de EXECUTAR AS QUERIES.

Isso identifica se o gargalo esta no connect() (ex.: reverse DNS do MySQL)
ou nas consultas em si.

Uso:  python3 mede_conexao.py
"""
import time

from functions.pesquisa_operadora import get_conn, consulta_operadora, get_conn_persistente

TELEFONE = "11987069513"
N = 5


def media(lista):
    return sum(lista) / len(lista) if lista else 0


print("=== 1. Tempo para ABRIR a conexao ===")
tempos_conn = []
for i in range(N):
    inicio = time.perf_counter()
    conn = get_conn()
    ms = (time.perf_counter() - inicio) * 1000
    tempos_conn.append(ms)
    conn.close()
    print(f"  connect {i + 1}: {ms:8.2f} ms")
print(f"  MEDIA CONNECT: {media(tempos_conn):.2f} ms")


print("\n=== 2. Consulta completa REUTILIZANDO a conexao ===")
conn = get_conn()
tempos_query = []
for i in range(N):
    inicio = time.perf_counter()
    consulta_operadora(TELEFONE, conn=conn)
    ms = (time.perf_counter() - inicio) * 1000
    tempos_query.append(ms)
    print(f"  consulta {i + 1}: {ms:8.2f} ms")
conn.close()
print(f"  MEDIA QUERY: {media(tempos_query):.2f} ms")


print("\n=== 3. Consulta ABRINDO conexao a cada vez (codigo antigo) ===")
tempos_total = []
for i in range(N):
    inicio = time.perf_counter()
    consulta_operadora(TELEFONE)  # sem conn -> abre e fecha
    ms = (time.perf_counter() - inicio) * 1000
    tempos_total.append(ms)
    print(f"  consulta {i + 1}: {ms:8.2f} ms")
print(f"  MEDIA TOTAL: {media(tempos_total):.2f} ms")


print("\n=== 4. Consulta com conexao PERSISTENTE (codigo novo) ===")
tempos_persist = []
for i in range(N):
    inicio = time.perf_counter()
    consulta_operadora(TELEFONE, conn=get_conn_persistente())
    ms = (time.perf_counter() - inicio) * 1000
    tempos_persist.append(ms)
    print(f"  consulta {i + 1}: {ms:8.2f} ms")
print(f"  MEDIA PERSISTENTE: {media(tempos_persist):.2f} ms")


print("\n" + "=" * 55)
print("RESUMO")
print("=" * 55)
print(f"  Abrir conexao ............ {media(tempos_conn):8.2f} ms")
print(f"  Queries (conexao pronta) . {media(tempos_query):8.2f} ms")
print(f"  Codigo antigo (total) .... {media(tempos_total):8.2f} ms")
print(f"  Codigo novo (persistente)  {media(tempos_persist):8.2f} ms")

if media(tempos_conn) > 100:
    print("\n  >>> ABRIR CONEXAO e o gargalo principal.")
    print("      Suspeita: reverse DNS do MySQL a cada conexao.")
    print("      Verifique no servidor MySQL:  SHOW VARIABLES LIKE 'skip_name_resolve';")
    print("      Se estiver OFF, peca ao DBA para ligar (skip_name_resolve=ON no my.cnf).")
elif media(tempos_query) > 100:
    print("\n  >>> As QUERIES sao o gargalo (inesperado, dado o teste anterior).")
else:
    print("\n  >>> Banco esta rapido nos dois casos.")
    print("      O gargalo dos 670ms esta na aplicacao (Django/Gunicorn/middleware),")
    print("      nao no banco.")