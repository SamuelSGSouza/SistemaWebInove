from django.shortcuts import render
from django.shortcuts import render, redirect
from django.urls import reverse
from .models import *
from functions.utils import *
from django.views.generic import TemplateView
from django.http import JsonResponse
import os, datetime,threading, pandas as pd
from django.http import StreamingHttpResponse
from django.contrib.auth.mixins import LoginRequiredMixin
import traceback, shutil
from django.http import FileResponse
from functions.contantes import *
from functions.gerador import inicia_gerador, inicia_gerador_mailing_2026, inicia_gerador_arquivos_cpf
from functions.finaliza_analise_de_dados import conta_dados
import json
import re
import unicodedata
from collections import defaultdict

from django.db.models import Max
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db.models.functions import TruncDate
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from functions.importa_dados_telefones import cadastra_telefones_dia, cadastra_telefones_antigos, pesquisa_telefones, imprime_relatorio
from functions.pesquisa_operadora import consulta_operadora, consulta_operadora_lote
import hmac
from dotenv import load_dotenv
import os
load_dotenv()

titulos = {
    'oi': "Mailing Original (Nio)",
    'geral': "Mailing Original",
    'giga_mais': "Mailing Giga +",
    'janeiro_2026': "Mailing Restrito",
    'mailing_cpfs': "Mailing CPF",
}


ESTADOS_NOMES = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
    "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
    "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins",
}
# from django.utils import timezone
# imprime_relatorio()
# telefones_achados = TelefonesDiscados.objects.values_list("telefone", flat=True)
# print("Telefones: ", len(telefones_achados))
# print("Telefones filtrados: ", len(set(telefones_achados)))

# limite = timezone.now() - datetime.timedelta(days=90)
# TelefonesDiscados.objects.filter(momento_chamada__lt=limite)
# print("Telefones Antigos: ", TelefonesDiscados.objects.filter(momento_chamada__lt=limite).count())
def _normaliza(texto: str) -> str:
    """Remove acentos e baixa a caixa para casar títulos de forma robusta."""
    texto = unicodedata.normalize("NFKD", texto or "")
    return texto.encode("ascii", "ignore").decode("ascii").lower()


def _classifica_titulo(titulo: str):
    """
    Lê um título de DadoExtracao e devolve (estado, segmento, viabilidade, credito)
    quando ele descreve uma contagem de CNPJs por status de crédito.
    Devolve None para qualquer outra linha (totais, viabilidade agregada, etc).

        segmento   -> 'mei' | 'nmei'
        viabilidade-> 'primaria' | 'secundaria'
        credito    -> 'aprovado' | 'negado' | 'sem_info'
    """
    t = _normaliza(titulo)

    if "cnpjs com viabilidade" not in t:
        return None

    m_estado = re.search(r"estado\s+([a-z]{2})\b", t)
    if not m_estado:
        return None
    estado = m_estado.group(1).upper()

    if "nao mei" in t:          # precisa vir antes de "mei"
        segmento = "nmei"
    elif "mei" in t:
        segmento = "mei"
    else:
        return None

    if "primaria" in t:
        viabilidade = "primaria"
    elif "secundaria" in t:
        viabilidade = "secundaria"
    else:
        return None

    if "credito aprovado" in t:
        credito = "aprovado"
    elif "credito negado" in t:
        credito = "negado"
    elif "sem info" in t:       # "sem infos de credito"
        credito = "sem_info"
    else:
        return None

    return estado, segmento, viabilidade, credito


def monta_payload_dashboard(registros):
    """
    Recebe a lista de DadoExtracao (mais recentes por título) e devolve a
    estrutura consumida pelo front:

        {
          "mei":  {"estados": [ {uf, nome, primaria, secundaria,
                                 aprovado, negado, sem_info, total,
                                 viab_primaria, viab_secundaria}, ... ],
                   "totais":  { ...mesmos campos do estado... }},
          "nmei": { ... }
        }
    """
    def _credito_zero():
        return {"aprovado": 0, "negado": 0, "sem_info": 0}

    bruto = {
        "mei": defaultdict(lambda: {"primaria": _credito_zero(), "secundaria": _credito_zero()}),
        "nmei": defaultdict(lambda: {"primaria": _credito_zero(), "secundaria": _credito_zero()}),
    }

    for reg in registros:
        info = _classifica_titulo(reg.titulo)
        if not info:
            continue
        estado, seg, viab, cred = info
        bruto[seg][estado][viab][cred] += reg.quantidade or 0

    payload = {}
    for seg in ("mei", "nmei"):
        estados_lista = []
        tot_prim = _credito_zero()
        tot_sec = _credito_zero()

        for uf, dados_uf in bruto[seg].items():
            prim, sec = dados_uf["primaria"], dados_uf["secundaria"]
            for c in ("aprovado", "negado", "sem_info"):
                tot_prim[c] += prim[c]
                tot_sec[c] += sec[c]

            aprovado = prim["aprovado"] + sec["aprovado"]
            negado = prim["negado"] + sec["negado"]
            sem_info = prim["sem_info"] + sec["sem_info"]
            estados_lista.append({
                "uf": uf,
                "nome": ESTADOS_NOMES.get(uf, uf),
                "primaria": prim,
                "secundaria": sec,
                "aprovado": aprovado,
                "negado": negado,
                "sem_info": sem_info,
                "total": aprovado + negado + sem_info,
                "viab_primaria": sum(prim.values()),
                "viab_secundaria": sum(sec.values()),
            })

        estados_lista.sort(key=lambda x: x["total"], reverse=True)

        aprovado = tot_prim["aprovado"] + tot_sec["aprovado"]
        negado = tot_prim["negado"] + tot_sec["negado"]
        sem_info = tot_prim["sem_info"] + tot_sec["sem_info"]
        payload[seg] = {
            "estados": estados_lista,
            "totais": {
                "uf": "",
                "nome": "Todos os estados",
                "primaria": tot_prim,
                "secundaria": tot_sec,
                "aprovado": aprovado,
                "negado": negado,
                "sem_info": sem_info,
                "total": aprovado + negado + sem_info,
                "viab_primaria": sum(tot_prim.values()),
                "viab_secundaria": sum(tot_sec.values()),
            },
        }

    return payload


class Dashboard(LoginRequiredMixin, TemplateView):
    template_name = "dashboard.html"

    def get_context_data(self, **kwargs):
        verifica_atualizacao_receita()
        ctx = super().get_context_data(**kwargs)
        sistema = "oi"

        # limpeza herdada do código antigo
        DadoExtracao.objects.filter(
            titulo="Total Empresas Receita Federal", sistema=sistema
        ).delete()

        # ---- 1 query: pega o registro mais recente de cada título -----------
        ids_recentes = (
            DadoExtracao.objects
            .filter(sistema=sistema)
            .values("titulo")
            .annotate(ult=Max("id"))
            .values_list("ult", flat=True)
        )
        registros = list(DadoExtracao.objects.filter(id__in=list(ids_recentes)))

        # ---- estrutura segmentada (MEI / NMEI) pro front --------------------
        payload = monta_payload_dashboard(registros)
        ctx["dashboard_json"] = json.dumps(payload)

        # KPIs gerais (RF)
        total_rf = next(
            (r for r in registros
             if r.titulo == "Total de Empresas ATIVAS somantos TODOS os estados"),
            None,
        )
        ctx["total_empresas"] = total_rf.quantidade if total_rf else 0

        # tabela de referência: só os registros mais recentes, ordenados
        ctx["dados"] = sorted(registros, key=lambda r: r.titulo)

        status = (
            Status_Execucoe_DB.objects
            .filter(sistema="geral").order_by("-id").first()
        )
        if status:
            ctx["ultima_exec"] = status.momento_inicializacao

        return ctx

class Status_Execucao(LoginRequiredMixin,TemplateView):
    template_name = "status_execucao.html"
    def get_context_data(self, **kwargs):
        context =  super().get_context_data(**kwargs)

        sistema = self.request.GET.get("sistema", "geral")
        if not sistema:
            sistema = self.request.GET.get("sistema", "geral")
        titulos = {
            'oi': "Mailing Original (Nio)",
            'geral': "Mailing Original",
            'giga_mais': "Mailing Giga +",
            'janeiro_2026': "Mailing Restrito",
            'mailing_cpfs': "Mailing CPF",
        }
        context["sistema"] = sistema
        if sistema == "janeiro_2026":
            context["is_janeiro"] = True
        if sistema == "giga_mais":
            context["is_giga_mais"] = True
        if sistema == "mailing_cpfs":
            context["is_mailing_cpfs"] = True
        if sistema == "geral":
            context["is_mailing_geral"] = True
        context['titulo'] = titulos[sistema]
        self.request.session["sistema"] = sistema

        context["acompanhar_gerador_activate"] = "active"

        possiveis_status = Status_Execucoe_DB.objects.filter(sistema=sistema).order_by("-id")
        if possiveis_status.exists():
            status = possiveis_status[0]
            context["data_inicializacao"] = status.momento_inicializacao
            context["data_finalizacao"] = status.momento_finalizacao

            context["fases"] = [f for f in Fase_Execucao_DB.objects.filter(status_execucao=status).order_by("-id")]
            context["fase_atual"] = context["fases"][len(context["fases"])-1].titulo
        return context
    
    def post(self, *args, **kwargs):
        arquivos = self.request.FILES.getlist('arquivo')

        pasta_destino = os.path.join(os.getcwd(), "media_janeiro_2026", f"arquivos_dfv")
        os.makedirs(pasta_destino, exist_ok=True)

        for path in os.listdir(pasta_destino):
            file = os.path.join(pasta_destino, path)
            if os.path.isfile(file):
                os.remove(file)
            elif os.path.isdir(file):
                shutil.rmtree(file)

        sucessos = []
        erros = []
        links = []
        relatorio = []

        total_arqs = 0
        for arquivo in arquivos:
            destino = os.path.join(pasta_destino, arquivo.name)
            with open(destino, 'wb+') as dest:
                for chunk in arquivo.chunks():
                    dest.write(chunk)
            total_arqs += 1
            sucesso, mensagem = verifica_arquivo(self.request,arquivo, destino, "arquivos_dfv", sistema="janeiro_2026")
            if sucesso:
                sucessos.append(mensagem)
            else:
                erros.append(mensagem)
        
        context = {}
        sistema="janeiro_2026"
        context["sistema"] = sistema
        context["is_janeiro"] = True
        context["titulo"] = "Mailing Restrito"
        self.request.session["sistema"] = sistema

        context["acompanhar_gerador_activate"] = "active"

        possiveis_status = Status_Execucoe_DB.objects.filter(sistema=sistema).order_by("-id")
        if possiveis_status.exists():
            status = possiveis_status[0]
            context["data_inicializacao"] = status.momento_inicializacao
            context["data_finalizacao"] = status.momento_finalizacao

            context["fases"] = [f for f in Fase_Execucao_DB.objects.filter(status_execucao=status).order_by("-id")]
            context["fase_atual"] = context["fases"][len(context["fases"])-1].titulo
        return render(self.request, self.template_name, context=context)

# dados = [['Total de empresas Ativas Meis no estado federal/MG', 1814017], ['Total de empresas Ativas NÃO Meis no estado federal/MG', 1092601], ['Total de empresas Ativas Meis no estado federal/TO', 107953], ['Total de empresas Ativas NÃO Meis no estado federal/TO', 73478], ['Total de empresas Ativas Meis no estado federal/ES', 388652], ['Total de empresas Ativas NÃO Meis no estado federal/ES', 291255], ['Total de empresas Ativas Meis no estado federal/SC', 849553], ['Total de empresas Ativas NÃO Meis no estado federal/SC', 661250], ['Total de empresas Ativas Meis no estado federal/PA', 326359], ['Total de empresas Ativas NÃO Meis no estado federal/PA', 204691], ['Total de empresas Ativas Meis no estado federal/AP', 29232], ['Total de empresas Ativas NÃO Meis no estado federal/AP', 23259], ['Total de empresas Ativas Meis no estado federal/MT', 340693], ['Total de empresas Ativas NÃO Meis no estado federal/MT', 224477], ['Total de empresas Ativas Meis no estado federal/RS', 1058683], ['Total de empresas Ativas NÃO Meis no estado federal/RS', 686749], ['Total de empresas Ativas Meis no estado federal/GO', 625067], ['Total de empresas Ativas NÃO Meis no estado federal/GO', 415746], ['Total de empresas Ativas Meis no estado federal/SP', 4853509], ['Total de empresas Ativas NÃO Meis no estado federal/SP', 3640312], ['Total de empresas Ativas Meis no estado federal/RN', 206158], ['Total de empresas Ativas NÃO Meis no estado federal/RN', 104181], ['Total de empresas Ativas Meis no estado federal/PB', 230985], ['Total de empresas Ativas NÃO Meis no estado federal/PB', 118993], ['Total de empresas Ativas Meis no estado federal/AM', 186088], ['Total de empresas Ativas NÃO Meis no estado federal/AM', 102263], ['Total de empresas Ativas Meis no estado federal/MS', 236921], ['Total de empresas Ativas NÃO Meis no estado federal/MS', 142567], ['Total de empresas Ativas Meis no estado federal/BA', 816523], ['Total de empresas Ativas NÃO Meis no estado federal/BA', 474885], ['Total de empresas Ativas Meis no estado federal/CE', 481848], ['Total de empresas Ativas NÃO Meis no estado federal/CE', 273189], ['Total de empresas Ativas Meis no estado federal/MA', 226996], ['Total de empresas Ativas NÃO Meis no estado federal/MA', 154741], ['Total de empresas Ativas Meis no estado federal/RR', 32696], ['Total de empresas Ativas NÃO Meis no estado federal/RR', 17788], ['Total de empresas Ativas Meis no estado federal/RO', 103020], ['Total de empresas Ativas NÃO Meis no estado federal/RO', 69561], ['Total de empresas Ativas Meis no estado federal/SE', 106979], ['Total de empresas Ativas NÃO Meis no estado federal/SE', 66684], ['Total de empresas Ativas Meis no estado federal/RJ', 1505523], ['Total de empresas Ativas NÃO Meis no estado federal/RJ', 813805], ['Total de empresas Ativas Meis no estado federal/AL', 162794], ['Total de empresas Ativas NÃO Meis no estado federal/AL', 76124], ['Total de empresas Ativas Meis no estado federal/PE', 496708], ['Total de empresas Ativas NÃO Meis no estado federal/PE', 249336], ['Total de empresas Ativas Meis no estado federal/DF', 255061], ['Total de empresas Ativas NÃO Meis no estado federal/DF', 224299], ['Total de empresas Ativas Meis no estado federal/AC', 32922], ['Total de empresas Ativas NÃO Meis no estado federal/AC', 22479], ['Total de empresas Ativas Meis no estado federal/PI', 150575], ['Total de empresas Ativas NÃO Meis no estado federal/PI', 90456], ['Total de empresas Ativas Meis no estado federal/PR', 1137338], ['Total de empresas Ativas NÃO Meis no estado federal/PR', 846110], ['Total Empresas MEI na Receita Federal', 16762853], ['Total Empresas NMEI na Receita Federal', 11161279], ['Total Empresas Receita Federal', 27924132]]
# for dado in dados:
#     salva_dado(dado[0], dado[1])

def download_arquivo_view(request):
    path = request.GET.get("full_path")
    if not os.path.exists(path) or not os.path.isfile(path):
        return JsonResponse({'status': 'error', 'message': 'Pasta não encontrada.'}, status=404)
    
    def file_iterator(file_path, chunk_size=8192):
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    response = StreamingHttpResponse(file_iterator(path), content_type='application/zip')
    response['Content-Disposition'] = f'attachment; filename="Arquivo_Tratado.zip"'
    return response

class TratamentosArquivosExternos(LoginRequiredMixin,TemplateView):
    template_name = "tratamento_arquivos_externos.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["tipo_tratamento"] = self.request.GET.get("tipo_tratamento")

        dict_tipos = {
            "Limpeza de BlackList": "Envie aqui um arquivo para que sejam removidos os telefones que estão na BlackList e Quarentena",
            "Enriquecimento de Dados": "Envie aqui um arquivo com coluna 'cnpj' e ele será enriquecido com dados do sistema",
            "Classificar Telefones": "Envie aqui um arquivo com coluna 'telefone' e ele será mapeado entre 'Atendido', 'Não Atendidos' e 'Novos'",
        }
        context["descricao"] = dict_tipos[context["tipo_tratamento"]]

        pasta = self.request.GET.get("pasta", "")
        self.request.session["pasta"] = pasta
        sistema = self.request.GET.get("sistema", "")
        self.request.session["sistema"] = sistema
        return context
    
    def post(self,request,*args, **kwargs):
        arquivos = request.FILES.getlist('arquivo')

        pasta_usuario = os.path.join(os.getcwd(), "media", f"{request.user.username}")
        pasta_destino = os.path.join( pasta_usuario, "arquivos_externos",)
        
        os.makedirs(pasta_destino, exist_ok=True)

        for path in os.listdir(pasta_destino):
            file = os.path.join(pasta_destino, path)
            if os.path.isfile(file):
                os.remove(file)
            elif os.path.isdir(file):
                shutil.rmtree(file)

        sucessos = []
        erros = []
        links = []
        relatorio = []
        erros_internos= None
        
        total_arqs = 0
        for arquivo in arquivos:
            destino = os.path.join(pasta_destino, arquivo.name)
            with open(destino, 'wb+') as dest:
                for chunk in arquivo.chunks():
                    dest.write(chunk)
            total_arqs += 1
            sucesso, mensagem = verifica_arquivo(request,arquivo, destino, "", "")
            if sucesso:
                sucessos.append(mensagem)
            else:
                erros.append(mensagem)
        pasta_raiz = os.path.join(os.getcwd(), "media")
        tipo_tratamento = self.request.GET.get("tipo_tratamento")
        if tipo_tratamento == "Limpeza de BlackList":
            zip_path = os.path.join(pasta_usuario,"arquivos_filtragem.zip")
            relatorio, erros_internos = filtra_arquivos(pasta_raiz, pasta_destino, pasta_usuario)
        
        if tipo_tratamento == "Enriquecimento de Dados":
            zip_path = os.path.join(pasta_usuario,"arquivos_complementar.zip")
            relatorio, erros_internos = complementa_arquivos(pasta_usuario,pasta_destino)
        
        if tipo_tratamento == "Classificar Telefones":
            zip_path = os.path.join(pasta_usuario,"arquivos_telefones_classificados.zip")
            relatorio, erros_internos = classifica_telefones(pasta_usuario,pasta_destino)
        
        

        relatorio = relatorio.split("\n")
        url_path = reverse('download_arquivo')  # ou reverse('minha_rota', kwargs={'pasta': pasta}) se tiver parâmetros nomeados
        url_relativa = f"{url_path}?full_path={zip_path}"
        url_completa = request.build_absolute_uri(url_relativa)
        

        if erros_internos:
            for er in erros_internos:
                erros.append(er) 

        ctx = self.get_context_data()
        ctx["show_modal"] = True
        ctx["modal_type"] = "success" if not erros else "error"
        ctx["messages"] = relatorio if not erros else erros
        ctx["download_url"] = url_completa
        return render(request, self.template_name, ctx)


class AtualizaBases(LoginRequiredMixin, TemplateView):
    template_name = "atualizacao_bases.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["base"] = self.request.GET.get("base")
        if context["base"] == "Quarentena":
            context["base_quarentena"] = True
        dict_tipos = {
            "BlackList": "Base de telefones em BlackList que NUNCA devem ser utilizados",
            "Quarentena": "Base de telefones que ficarão em quarentena por determinado período até poderem ser utilizados.",
            "Credito": "Base de crédito a ser verificado no mailing",
            "Telefone": "Base de telefones a serem usados no enriquecimento",
            "Mailing Restrito": "Envie aqui os arquivos de mailing restrito para iniciar a geração de um novo mailing.",
            "Giga Mais": "Envie aqui os arquivos de mailing para iniciar a geração de um novo mailing da Giga +.",
            "CPF Externo": "Base de dados de cpf's coletados externamente para serem usados no mailing de cpf",
            "CPF CredLink": "Base de dados de cpf's coletados na credlink para serem usados no mailing de cpf. \n AVISO: Garanta que o nome de cada arquivo contenha a uf do respectivo estado, ex: RJ, SP, SC...",
        }
        context["descricao"] = dict_tipos[context["base"]]
        return context

    def post(self,request,*args, **kwargs):
        arquivos = request.FILES.getlist('arquivo')
        base = self.request.GET.get("base")
        excluir_anteriores = self.request.POST.get("excluir_anteriores")
        print(f"Excluir Anteriores: {excluir_anteriores}")
        PASTAS_RAIZ = {
            "BlackList": "arquivos_blacklist",
            "Quarentena": "arquivos_quarentena",
            "Mailing Restrito": "arquivos_dfv",
            "Giga Mais": "arquivos_dfv",
            "Credito": "arquivos_credito",
            "Telefone": "arquivos_enriquecimento",
            "CPF Externo": "arquivos_cpf_externo",
            "CPF CredLink": "arquivos_cpf_credlink",
        }
        if base == "Mailing Restrito":
            pasta_media = "media_janeiro_2026"  
        elif base ==  "Giga Mais":
            pasta_media = "media_giga_mais"  
        elif base ==  "CPF Externo" or base == "CPF CredLink":
            pasta_media = "media_mailing_cpf"  
        else: 
            pasta_media = "media"
        
        print(f"Base {base}")
        print(f"Arquivos {arquivos}")
        pasta_destino = os.path.join(os.getcwd(), pasta_media, PASTAS_RAIZ[base])
        os.makedirs(pasta_destino, exist_ok=True)
        
        if base in ["BlackList", "Mailing Restrito", "Giga Mais", "CPF Externo"] or str(excluir_anteriores) == "on":
            print("Excluindo anteriores")
            for path in os.listdir(pasta_destino):
                
                file = os.path.join(pasta_destino, path)
                print(f"Analisando: {file}")
                if os.path.isfile(file):
                    os.remove(file)
                elif os.path.isdir(file):
                    shutil.rmtree(file)

        sucessos = []
        erros = []
        links = []
        relatorio = []

        sistema = self.request.session.get("sistema", "")

        total_arqs = 0
        for arquivo in arquivos:
            destino = os.path.join(pasta_destino, arquivo.name)
            with open(destino, 'wb+') as dest:
                for chunk in arquivo.chunks():
                    dest.write(chunk)
            total_arqs += 1
            sucesso, mensagem = verifica_arquivo(request,arquivo, destino, PASTAS_RAIZ[base], sistema)
            if sucesso:
                sucessos.append(mensagem)
            else:
                erros.append(mensagem)


        if PASTAS_RAIZ[base] == "arquivos_quarentena":
            relatorio, erros_internos = gera_e_atualiza_quarentena(os.path.join(os.getcwd(), "media"), "")
            relatorio = relatorio.split("\n")

            if erros_internos:
                for er in erros_internos:
                    erros.append(er)

        if PASTAS_RAIZ[base] == "arquivos_enriquecimento":
            relatorio, erros_internos = gera_e_atualiza_enriquecimento()
            relatorio = relatorio.split("\n")

            if erros_internos:
                for er in erros_internos:
                    erros.append(er)

        if PASTAS_RAIZ[base] == "arquivos_credito":
            relatorio, erros_internos = gera_e_atualiza_dados_credito_turbo("media")
            relatorio = relatorio.split("\n")

            if erros_internos:
                for er in erros_internos:
                    erros.append(er)

        if PASTAS_RAIZ[base] == "arquivos_dfv":
            tipos ={
                "Mailing Restrito": "janeiro_2026",
                "Giga Mais": "giga_mais"
            }
            processo = threading.Thread(target=inicia_gerador_mailing_2026, args=(tipos[base],))
            processo.start()
            relatorio.append(f"Sistema {base} iniciado com sucesso!")

        if PASTAS_RAIZ[base] == "arquivos_cpf_externo":
            
            processo = threading.Thread(target=inicia_gerador_arquivos_cpf)
            processo.start()
            relatorio.append(f"Sistema {base} iniciado com sucesso!")

        ctx = self.get_context_data()
        ctx["show_modal"] = True
        ctx["modal_type"] = "success" if not erros else "error"
        ctx["messages"] = relatorio if not erros else erros
        # ctx["download_url"] = ""
        return render(request, self.template_name, ctx)

def filtra_mailing_cpfs_view(request):    


    context = {
        'estados': ESTADOS_BR,
        'resultados': None,
        'qtd_resultados': 0,
        'colunas': [],
        'sistema': "mailing_cpfs"
    }
    nome_padrao_arquivo = ""
    pasta_raiz =  os.path.join(os.getcwd(), "media_mailing_cpf")

    sistema = "mailing_cpfs"

    filepath_csv = os.path.join(os.getcwd(), f"{pasta_raiz}/{request.user.username}_arquivos_mailing_filtrados")
    os.makedirs(filepath_csv, exist_ok=True)

    
    for file in os.listdir(filepath_csv):
        os.remove(os.path.join(filepath_csv, file))

    if request.method == 'POST':
        try:
            # Processar filtros
            filtros = {}            

            # Estados (múltiplos valores via checkbox)
            estados = request.POST.getlist('estado', [])
            print("ESTADOS: ", estados)
            if estados != []:
                estados = estados
            else:
                estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
            if estados:
                filtros['uf'] = estados
                

            tipo_base = request.POST.get("tipoBase", "")
            if tipo_base and tipo_base != "Ambos":
                filtros["pasta"] = tipo_base


            conjunto_telefones = request.POST.get("conjuntoTelefone", "")
            tipos_telefone = request.POST.get("tipoTelefone", "")
            tipoMailing = request.POST.get("tipoMailing", "")
            filtro_telefone_blacklist = request.POST.get("filtro_telefone_blacklist", "")

            checkbox_credito_preaprovado = request.POST.get("checkbox_credito_preaprovado", "")
            checkbox_pre_negado = request.POST.get("checkbox_pre_negado", "")
            checkbox_sem_info_credito = request.POST.get("checkbox_sem_info_credito", "")

            # Obter dados do CSV
            
            
            pasta_dados = os.path.join(pasta_raiz, "viabilidades_credito_enriquecido")
            df = get_dados_mailing_cpf(filtros, conjunto_telefones=conjunto_telefones, tipos_telefone= tipos_telefone, tipoMailing=tipoMailing, filtro_telefone_blacklist=filtro_telefone_blacklist, pasta_dados=pasta_dados,)
            

            meses = {
                "1": "Janeiro",
                "2": "Fevereiro",
                "3": "Março",
                "4": "Abril",
                "5": "Maio",
                "6": "Junho",
                "7": "Julho",
                "8": "Agosto",
                "9": "Setembro",
                "10": "Outubro",
                "11": "Novembro",
                "12": "Dezembro"
            }
            dia = datetime.datetime.now().day 
            dia = str(dia) if dia > 9 else "0"+ str(dia)
            mes_extenso = meses[str(datetime.datetime.now().month)]
            data_atual = f'{dia}-{mes_extenso}'
            nome_padrao_arquivo += data_atual
            # Preparar dados para exibição
            
            max_linhas = 200_000

            if len(df.index) > max_linhas:
                # Divide em pedaços de 200k
                f = 0
                for i in range(0, len(df), max_linhas):
                    nome_arquivo = nome_padrao_arquivo + f"_parte_{f}" + ".csv"
                    df.iloc[i:i + max_linhas].to_csv(os.path.join(filepath_csv, nome_arquivo),sep=";", index=False)
                    f+=1
            else:
                df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

            zip_folder(filepath_csv, f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip")

            context['resultados'] = df.replace({pd.NA: ''}).head(50).values.tolist()
            context['colunas'] = df.columns.tolist()
            context['qtd_resultados'] = len(df.index)

            filename = f"Mailing CPF's - {dia} de {mes_extenso}.zip"

            response = FileResponse(open(f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip", 'rb'), as_attachment=True, filename=filename)
            return response
        except Exception as e:
            return JsonResponse({"error": traceback.format_exc()})

    return render(request, 'filtra_mailing_cpfs.html', context)

def filtra_mailing_view(request):    


    context = {
        'nome_dados': 'Empresas',
        'estados': ESTADOS_BR,
        'resultados': None,
        'qtd_resultados': 0,
        'colunas': [],
        'cnaes': get_cnaes(),
        'sistema': request.session["sistema"],
        'cidades':LISTA_ESTADOS_MUNICIPIOS
    }
    nome_padrao_arquivo = "Dados Mailing"
    PASTAS_RAIZ = {
            "oi": os.path.join(os.getcwd(), "media"),
            "geral": os.path.join(os.getcwd(), "media"),
            "giga_mais": os.path.join(os.getcwd(), "media_giga_mais"),
            "janeiro_2026": os.path.join(os.getcwd(), "media_janeiro_2026"),
            "mailing_cpfs": os.path.join(os.getcwd(), "media_mailing_cpf")
        }
    sistema = request.session["sistema"]
    pasta_raiz = PASTAS_RAIZ[sistema]

    filepath_csv = os.path.join(os.getcwd(), f"{pasta_raiz}/{request.user.username}_arquivos_mailing_filtrados")
    os.makedirs(filepath_csv, exist_ok=True)

    
    for file in os.listdir(filepath_csv):
        os.remove(os.path.join(filepath_csv, file))
    if request.method == 'POST':
        try:
            # Processar filtros
            filtros = {}

            formato_saida = request.POST.get("formato_saida", "Padrão")
            

            # Estados (múltiplos valores via checkbox)
            estados = request.POST.getlist('estado', [])
            print("ESTADOS: ", estados)
            if estados != []:
                estados = estados
            else:
                estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
            if estados:
                filtros['uf'] = estados
                
            cidades = request.POST.getlist("cidade_list",  [])
            if cidades:
                if "cpf" in sistema:
                    filtros["cidade"] = cidades
                else:
                    filtros["municipio"] = cidades
            print(f"CIDADES: ", cidades)

            # # CNAE (múltiplos valores)
            cnaes = request.POST.getlist('cnae_list', [])
            if cnaes:
                filtros['cnae_fiscal'] = cnaes

            # # Município (múltiplos valores)
            # municipios_raw = request.POST.get('municipio', '')
            # municipios = [m.strip() for m in municipios_raw.split(',') if m.strip()]
            # if municipios:
            #     filtros['municipio'] = municipios

            # # Bairro (múltiplos valores)
            # bairros_raw = request.POST.get('bairro', '')
            # bairros = [b.strip() for b in bairros_raw.split(',') if b.strip()]
            # if bairros:
            #     filtros['bairro'] = bairros

            # termos_chave = request.POST.get("termos_chave", "")
            # if termos_chave:
            #     filtros["termos_chave"] = termos_chave

            tipo_empresa = request.POST.get("tipoEmpresa", "")
            if tipo_empresa and tipo_empresa != "Ambos":
                filtros["MEINAOMEI"] = tipo_empresa


            conjunto_telefones = request.POST.get("conjuntoTelefone", "")
            tipos_telefone = request.POST.get("tipoTelefone", "")
            tipoMailing = request.POST.get("tipoMailing", "")
            filtro_telefone_blacklist = request.POST.get("filtro_telefone_blacklist", "")
            coletar_atendidos = request.POST.get("checkbox_coletar_atendidos", False)
            if coletar_atendidos:
                coletar_atendidos = True

            coletar_nao_atendidos = request.POST.get("checkbox_coletar_nao_atendidos", False)
            if coletar_nao_atendidos:
                coletar_nao_atendidos = True
                
            coletar_novos = request.POST.get("checkbox_coletar_novos", False)
            if coletar_novos:
                coletar_novos = True

            checkbox_credito_preaprovado = request.POST.get("checkbox_credito_preaprovado", "")
            checkbox_pre_negado = request.POST.get("checkbox_pre_negado", "")
            checkbox_sem_info_credito = request.POST.get("checkbox_sem_info_credito", "")

            # Obter dados do CSV
            dfs = []
            tipos_credito = []
            if checkbox_credito_preaprovado:
                tipos_credito.append("Aprovado")
                

            if checkbox_pre_negado:
                tipos_credito.append("Negado")

            if checkbox_sem_info_credito:
                tipos_credito.append("Sem Infos")


            if len(tipos_credito) == 0:
                df = pd.DataFrame()
                df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

                context['resultados'] = 0
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)

                return render(request, 'data_not_found.html', context)

            if sistema != "giga_mais":
                filtros["credito"] = tipos_credito
            
            
            pasta_dados = os.path.join(pasta_raiz, "viabilidades_credito_enriquecido")
            df = get_dados_mailing(filtros, tipos_credito=tipos_credito, formato_saida=formato_saida, conjunto_telefones=conjunto_telefones, tipos_telefone= tipos_telefone, tipoMailing=tipoMailing, filtro_telefone_blacklist=filtro_telefone_blacklist, pasta_dados=pasta_dados, coletar_atendidos=coletar_atendidos, coletar_nao_atendidos=coletar_nao_atendidos, coletar_novos=coletar_novos)
            dfs.append(df)

            df = pd.concat(dfs)
            df.drop_duplicates(subset=["cnpj"], keep="first",inplace=True)
            

            meses = {
                "1": "Janeiro",
                "2": "Fevereiro",
                "3": "Março",
                "4": "Abril",
                "5": "Maio",
                "6": "Junho",
                "7": "Julho",
                "8": "Agosto",
                "9": "Setembro",
                "10": "Outubro",
                "11": "Novembro",
                "12": "Dezembro"
            }
            dia = datetime.datetime.now().day 
            dia = str(dia) if dia > 9 else "0"+ str(dia)
            mes_extenso = meses[str(datetime.datetime.now().month)]
            data_atual = f'{dia}-{mes_extenso}'
            # Preparar dados para exibição
            if not df.empty:
                
                max_linhas = 200_000

                if len(df.index) > max_linhas:
                    # Divide em pedaços de 200k
                    f = 0
                    for i in range(0, len(df), max_linhas):
                        nome_arquivo = nome_padrao_arquivo + f"_parte_{f}" + ".csv"
                        df.iloc[i:i + max_linhas].to_csv(os.path.join(filepath_csv, nome_arquivo),sep=";", index=False)
                        f+=1
                else:
                    df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

                zip_folder(filepath_csv, f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip")

                context['resultados'] = df.replace({pd.NA: ''}).head(50).values.tolist()
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)
                

                filename = f"{titulos[sistema]} - {dia} de {mes_extenso}.zip"

                response = FileResponse(open(f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip", 'rb'), as_attachment=True, filename=filename)
                return response
            return render(request, 'data_not_found.html', context)

        except Exception as e:
            return JsonResponse({"error": traceback.format_exc()})

    return render(request, 'filtra_mailing.html', context)

def inicia_gerador_view(request):
    
    # if request.method != 'POST':
    #     return JsonResponse({'status': 'error', 'sucessos': [], "erros":['Método inválido.',], "links": [], "relatorio": []})

    sistema = request.GET.get("sistema", "oi")
    processo = threading.Thread(target=inicia_gerador, args=(sistema,))
    processo.start()

    return JsonResponse({'status': 'success', 'sucessos': [f"Iniciou sistema {sistema} com sucesso!",], "erros":[], "links": [], "relatorio": []})

def total_telefones_view(request):
    
    # if request.method != 'POST':
    #     return JsonResponse({'status': 'error', 'sucessos': [], "erros":['Método inválido.',], "links": [], "relatorio": []})

    sistema = request.GET.get("sistema", "oi")
    total =  TelefonesDiscados.objects.count()

    return JsonResponse({'status': 'success', 'sucessos': [f"Total de telefones: {total}",], "erros":[], "links": [], "relatorio": []})


def importa_dados_telefones_view(request):
    
    processo = threading.Thread(target=cadastra_telefones_dia, )
    processo.start()
    return JsonResponse({'status': 'success', 'sucessos': [f"Iniciou sistema coleta diária com sucesso!",], "erros":[], "links": [], "relatorio": []})

# def importa_dados_telefones_view(request):
#     processo = threading.Thread(target=cadastra_telefones_dia, )
#     processo.start()

#     return JsonResponse({'status': 'success', 'sucessos': [f"Iniciou sistema coleta diária com sucesso!",], "erros":[], "links": [], "relatorio": []})


def telefones_discados_view(request):
    qs = TelefonesDiscados.objects.all()
 
    # Agregações feitas direto no banco (uma única query)
    resumo = qs.aggregate(
        total=Count('id'),
        sucesso=Count('id', filter=Q(sucesso_chamada=True)),
        falha=Count('id', filter=Q(sucesso_chamada=False)),
        ultima_chamada=Max('momento_chamada'),
    )
 
    # Série diária para o gráfico de evolução
    por_dia = (
        qs.annotate(dia=TruncDate('momento_chamada'))
          .values('dia')
          .annotate(
              sucesso=Count('id', filter=Q(sucesso_chamada=True)),
              falha=Count('id', filter=Q(sucesso_chamada=False)),
          )
          .order_by('dia')
    )
 
    dashboard = {
        'sucesso': resumo['sucesso'] or 0,
        'falha': resumo['falha'] or 0,
        'serie': [
            {
                'dia': item['dia'].strftime('%d/%m/%Y'),
                'sucesso': item['sucesso'],
                'falha': item['falha'],
            }
            for item in por_dia
        ],
    }
 
    # Lista mais recente primeiro. Se a tabela crescer muito,
    # troque o [:1000] por paginação (django.core.paginator).
    telefones = qs.order_by('-momento_chamada')[:1000]
 
    context = {
        'resumo': resumo,
        'telefones': telefones,
        'dashboard_json': json.dumps(dashboard),
    }
    return render(request, 'telefones_discados.html', context)



def filtro_geral_view(request):
    context = {
        'nome_dados': 'Empresas',
        'estados_municipios': DICT_ESTADOS_MUNICIPIOS,
        'cnaes': get_cnaes()
    }

    nome_padrao_arquivo = "Dados Mailing"
    filepath_csv = os.path.join(os.getcwd(), "media/arquivos_receita_federal_filtrados")
    for file in os.listdir(filepath_csv):
        os.remove(os.path.join(filepath_csv, file))
    if request.method == 'POST':
        try:
            initial = time.time()
            # Processar filtros
            filtros = {}

            # Estados (múltiplos valores via checkbox)
            estados = request.POST.getlist('estado', [])
            if estados != []:
                estados = estados
            else:
                estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
                            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
                            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
                        ]
            if estados:
                filtros['uf'] = estados
                for estado in estados:
                    nome_padrao_arquivo += f" {estado} "



            # CNAE (múltiplos valores)
            cnaes = request.POST.getlist('cnae', '')
            if cnaes:
                filtros['cnae_fiscal'] = cnaes


            

            # Município (múltiplos valores)
            municipios = request.POST.getlist('municipio', '')
            
            if municipios:
                filtros['municipio'] = [m.split("|")[0] for m in municipios]
                
            # termos_chave = request.POST.get("termos_chave", "")
            # if termos_chave:
            #     filtros["termos_chave"] = termos_chave

            tipo_empresa = request.POST.get("tipo_mei", "")

            if tipo_empresa:
                filtros["MEINAOMEI"] = tipo_empresa
            
            # Obter dados do CSV
            df = get_dados_csv(filtros)
            # Converter colunas categóricas para strings
            for col in df.select_dtypes(include=['category']).columns:
                df[col] = df[col].astype(str)


            tipoTelefone = request.POST.get('tipoTelefone', '')

            if tipoTelefone == "apenas_movel":
                df = remove_fixos(df) 


            # Agora pode usar replace e fillna tranquilamente
            df["cnpj"] = df["cnpj"].apply(lambda x: re.sub(r'[^0-9]', '', x))
            df = padronizacao(df.replace(",0", "").replace(".0", "").fillna("")).reset_index(drop=True)

            meses = {
                "1": "Janeiro",
                "2": "Fevereiro",
                "3": "Março",
                "4": "Abril",
                "5": "Maio",
                "6": "Junho",
                "7": "Julho",
                "8": "Agosto",
                "9": "Setembro",
                "10": "Outubro",
                "11": "Novembro",
                "12": "Dezembro"
            }
            dia = datetime.datetime.now().day 
            dia = str(dia) if dia > 9 else "0"+ str(dia)
            data_atual = f'{dia} de {meses[str(datetime.datetime.now().month)]}'
            # Preparar dados para exibição
            
            formato_download = request.POST.get('formatoDownload', '')
            if formato_download == "IPBOX":
                print(df.columns.tolist())
                colunas_ipbox = ["cnpj","razao_social", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "DDD1", "TEL1", "DDD2", "TEL2", "DDD3", "TEL3", "DDD4", "TEL4", "DDD5", "TEL5", "DDD6", "TEL6", "DDD7", "TEL7", "DDD8", "TEL8", ]
                colunas_telefone = ["TEL1", "TEL2", "TEL3"] 

                i = 1
                for ct in colunas_telefone:
                    df[f'DDD{i}'] = df[ct].str[:2]
                    df[f"TEL{i}"] = df[ct].str[2:]
                    i+=1                
                for i in range(4,9):
                    df[f'DDD{i}'] = ""
                    df[f"TEL{i}"] = ""
                df = df[colunas_ipbox]


            if len(df.index) > 0:
                max_linhas = 200_000

                if len(df.index) > max_linhas:
                    # Divide em pedaços de 200k
                    f = 0
                    for i in range(0, len(df), max_linhas):
                        nome_arquivo = nome_padrao_arquivo + f"parte {f}" + ".csv"
                        df.iloc[i:i + max_linhas].to_csv(os.path.join(filepath_csv, nome_arquivo),sep=";", index=False)
                        f+=1
                else:
                    df.to_csv(os.path.join(filepath_csv, f"{nome_padrao_arquivo}.csv"), sep=";", index=False)

                zip_folder(filepath_csv, f"media/{request.user.username}_filtrados.zip")

                context['resultados'] = df.replace({pd.NA: ''}).head(50).values.tolist()
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)

            response = FileResponse(open(f"media/{request.user.username}_filtrados.zip", 'rb'), as_attachment=True, filename=f'Dados Receita Federal {data_atual}.zip')
            return response
        except Exception as e:
            return JsonResponse({"error": traceback.format_exc()})

    return render(request, 'filtro_geral.html', context)

def _extrai_token(request):
    """
    Extrai o token da requisição. Aceita:
      Header:  Authorization: Bearer <token>
      Header:  X-Api-Token: <token>
    """
    auth = request.META.get("HTTP_AUTHORIZATION", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):].strip()
 
    return request.META.get("HTTP_X_API_TOKEN", "").strip() or None
 
 
def exige_token(view_func):
    """Decorator que bloqueia a requisição se o token estiver ausente ou errado."""
    def wrapper(request, *args, **kwargs):
        API_TOKEN = os.getenv("API_TOKEN", "token_padrao")
        token = _extrai_token(request)
 
        if not token:
            return JsonResponse({"erro": "Token nao informado."}, status=401)
 
        # compare_digest evita timing attacks (comparação em tempo constante)
        if not hmac.compare_digest(token, API_TOKEN):
            return JsonResponse({"erro": "Token invalido."}, status=403)
 
        return view_func(request, *args, **kwargs)
 
    return wrapper
 
 
@exige_token
@require_http_methods(["GET"])
def api_consulta_telefone(request, telefone=None):
    """
    Endpoint de consulta individual.
 
    Aceita o telefone de duas formas:
      GET /api/consulta/11987069513/        (na URL)
      GET /api/consulta/?telefone=11987069513   (querystring)
 
    Resposta:
      {"telefone": "11987069513", "portado": true, "rn1": "...", "operadora": "TIM"}
    """
    telefone = telefone or request.GET.get("telefone")
 
    if not telefone:
        return JsonResponse(
            {"erro": "Informe o telefone na URL ou no parametro ?telefone="},
            status=400,
        )
 
    try:
        resultado = consulta_operadora(telefone)
    except ValueError as e:
        return JsonResponse({"erro": str(e)}, status=400)
    except Exception:
        return JsonResponse(
            {"erro": "Falha ao consultar o banco de dados."}, status=500
        )
 
    if resultado["rn1"] is None:
        return JsonResponse(
            dict(resultado, erro="Telefone nao encontrado nas bases."),
            status=404,
        )
 
    return JsonResponse(resultado)
 
 
@csrf_exempt
@exige_token
@require_http_methods(["POST"])
def api_consulta_lote(request):
    """
    Endpoint de consulta em lote.
 
    POST /api/consulta/lote/
    Body (JSON):
      {"telefones": ["11987069513", "21998765432", "..."]}
 
    Resposta:
      {
        "total": 2,
        "encontrados": 1,
        "resultados": [
          {"telefone": "11987069513", "portado": true, "rn1": "...", "operadora": "TIM"},
          {"telefone": "21998765432", "portado": null, "rn1": null, "operadora": null}
        ]
      }
    """
    try:
        body = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"erro": "Body invalido. Envie um JSON."}, status=400)
 
    telefones = body.get("telefones")
 
    if not isinstance(telefones, list) or not telefones:
        return JsonResponse(
            {"erro": 'Envie uma lista no formato {"telefones": ["...", "..."]}'},
            status=400,
        )
 
    MAX_LOTE = 1000
    if len(telefones) > MAX_LOTE:
        return JsonResponse(
            {"erro": "Maximo de {} telefones por requisicao.".format(MAX_LOTE)},
            status=400,
        )
 
    try:
        resultados = consulta_operadora_lote(telefones)
    except Exception:
        return JsonResponse(
            {"erro": "Falha ao consultar o banco de dados."}, status=500
        )
 
    encontrados = sum(1 for r in resultados if r.get("rn1") is not None)
 
    return JsonResponse(
        {
            "total": len(resultados),
            "encontrados": encontrados,
            "resultados": resultados,
        }
    )