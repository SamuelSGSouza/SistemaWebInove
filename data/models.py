from django.db import models

OPCOES_SISTEMA = (
        ("oi", "oi"),
        ("geral", "geral"),
        ("giga_mais", "giga_mais"),
        ("janeiro_2026", "janeiro_2026"),
    )

# Create your models here.
class DadosGerais(models.Model):
    empresas_na_receita_federal = models.IntegerField()
    empresas_com_viabilidade = models.IntegerField()
    
class Status_Execucoe_DB(models.Model):
    sistema = models.CharField(choices=OPCOES_SISTEMA,default="geral", max_length=255)
    momento_inicializacao = models.DateTimeField(auto_now=True)
    momento_finalizacao = models.DateTimeField(auto_now=True)


class Fase_Execucao_DB(models.Model):
    status_execucao = models.ForeignKey(Status_Execucoe_DB,on_delete=models.CASCADE)
    titulo = models.CharField(max_length=255)
    status = models.CharField(choices=(
        ("Pendente", "Pendente"),
        ("Concluido", "Concluido"),
        ("Em Andamento", "Em Andamento"),
        ("Erro", "Erro"),
    ),
    
    default="Pendente", max_length=255)
    color = models.CharField(choices=(
        ("primary","primary"),
        ("danger","danger"),
        ("secondary","secondary"),
        ("success", "success"),
    ),default="primary", max_length=255)

def salva_status(execucao:Status_Execucoe_DB, titulo, status):
    dict_color={
        "Concluido": "success",
        "Em Andamento": "primary",
        "Pendente": "secondary",
        "Erro": "danger"
    }

    Fase_Execucao_DB.objects.create(
        status_execucao=execucao,
        titulo=titulo,
        status=status,
        color=dict_color[status]
    )

class Log(models.Model):
    sistema = models.CharField(choices=OPCOES_SISTEMA,default="geral", max_length=255)
    log = models.TextField()
    momento_criacao = models.DateTimeField(auto_now=True)
    

def salva_log(msg,sistema):
    Log.objects.create(log=msg, sistema=sistema)


class DadoExtracao(models.Model):
    titulo = models.CharField(max_length=255)
    quantidade = models.IntegerField()
    momento_criacao = models.DateTimeField(auto_now=True)

def salva_dado(titulo, quantidade):
    DadoExtracao.objects.create(
        titulo=titulo,
        quantidade=quantidade,
    )