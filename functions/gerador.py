from functions.baixar_receita_federal import fase_1_gerador
from functions.concatena_receita_e_dfv import fase_2_concatenador
from functions.utils import verificador_fase_1

def inicia_gerador(sistema="oi"):
    # fase_1_gerador()
    # fase_1_ok = verificador_fase_1()
    # if fase_1_ok:
        fase_2_concatenador(sistema=sistema)