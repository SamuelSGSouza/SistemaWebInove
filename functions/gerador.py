from functions.baixar_receita_federal import fase_1_gerador
from functions.concatena_receita_e_dfv import fase_2_concatenador
from functions.define_credito import fase_3_define_credito
from functions.encontra_telefones_adicionais import fase_4_enriquecer
from data.models import Status_Execucoe_DB

def inicia_gerador(sistema="oi"):
    # fase_1_ok = fase_1_gerador()
    # if fase_1_ok:
        nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
        # fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)
        # if fase_2_ok:
        # fase_3_ok = fase_3_define_credito(sistema=sistema, nova_execucao=nova_execucao)
        #     if fase_3_ok:
        fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)