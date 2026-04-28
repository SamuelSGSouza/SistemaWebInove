from functions.baixar_receita_federal import fase_1_gerador
from functions.concatena_receita_e_dfv import fase_2_concatenador
from functions.define_credito import fase_3_define_credito
from functions.encontra_telefones_adicionais import fase_4_enriquecer
from data.models import Status_Execucoe_DB
from functions.finaliza_analise_de_dados import conta_dados
def inicia_gerador(sistema="oi"):
    # fase_1_ok = fase_1_gerador()
    # if fase_1_ok == True:
    nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
    fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)
    if fase_2_ok:
        fase_3_ok = fase_3_define_credito(sistema=sistema, nova_execucao=nova_execucao)
        if fase_3_ok:
            fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)
            conta_dados(sistema)

    inicia_gerador_mailing_2026()

    inicia_gerador_arquivos_cpf()


def inicia_gerador_mailing_2026(sistema="mailing_2026"):
    nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
    fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)
    if fase_2_ok:
        fase_3_ok = fase_3_define_credito(sistema=sistema, nova_execucao=nova_execucao)
        if fase_3_ok:
            fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)

def inicia_gerador_arquivos_cpf(sistema="mailing_cpfs"):
    nova_execucao = Status_Execucoe_DB.objects.create(sistema=sistema)
    fase_2_ok = fase_2_concatenador(sistema=sistema, nova_execucao=nova_execucao)
    if fase_2_ok:
        fase_4_enriquecer(sistema=sistema, nova_execucao=nova_execucao)