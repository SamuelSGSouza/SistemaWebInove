from django.db import models
from datetime import datetime
import requests
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
    momento_inicializacao = models.DateTimeField(auto_now_add=True)
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
    print(titulo)
    Fase_Execucao_DB.objects.filter(status_execucao=execucao, status="Em Andamento").update(status="Concluido")
    Fase_Execucao_DB.objects.create(
        status_execucao=execucao,
        titulo=titulo,
        status=status,
        color=dict_color[status]
    )
    execucao.save()
    
    print("status salvo com sucesso")

class Log(models.Model):
    sistema = models.CharField(choices=OPCOES_SISTEMA,default="geral", max_length=255)
    log = models.TextField()
    momento_criacao = models.DateTimeField(auto_now=True)
    

def salva_log(msg,sistema):
    print(msg, " ", sistema )
    Log.objects.create(log=msg, sistema=sistema)


class DadoExtracao(models.Model):
    sistema = models.CharField(choices=(
        ('oi', 'oi'),
        ('janeiro_2026', 'janeiro_2026'),
    ), max_length=255, default="oi")
    titulo = models.CharField(max_length=255)
    quantidade = models.IntegerField()
    momento_criacao = models.DateTimeField(auto_now=True)

def salva_dado(titulo, quantidade, sistema="oi"):
    
    DadoExtracao.objects.create(
        titulo=titulo,
        quantidade=quantidade,
        sistema=sistema
    )


class IniciacaoSistema(models.Model):
    mes_ano = models.CharField(max_length=255,unique=True)
    momento_criacao = models.DateTimeField(auto_now=True)

class ExecucaoSistema(models.Model):
    mes_ano = models.CharField(max_length=255,unique=True)
    momento_criacao = models.DateTimeField(auto_now=True)

def verifica_atualizacao_receita():
    mes = datetime.today().month
    mes = f"0{mes}" if len(str(mes)) < 2 else str(mes)
    year = datetime.today().year
    mes_ano = f"{year}-{mes}"
    response = requests.get(f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{mes_ano}/Cnaes.zip")
    if response.status_code == 200: #existe dados para esse mês
        if not ExecucaoSistema.objects.filter(mes_ano=mes_ano).exists():#sistema ainda não foi iniciado
            ExecucaoSistema.objects.create(mes_ano=mes_ano)
            #faz requisição pro endpoint de inicialização
            requests.get("http://177.39.236.250/data/inicia_gerador_view")