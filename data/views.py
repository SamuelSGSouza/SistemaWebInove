from django.shortcuts import render
from django.shortcuts import render, redirect
from django.urls import reverse
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
# from .forms import LoginForm
from .models import *
# from universal_data.models import ResultadoExtracao
# from functions.gera_mailing import *
from functions.utils import *
# from universal_data.models import *
from django.views.generic import TemplateView
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
import os, datetime,threading, pandas as pd
from django.conf import settings
from typing import Union
from django.http import StreamingHttpResponse
from multiprocessing import Process   
from django.contrib.auth.mixins import LoginRequiredMixin
import traceback, shutil
from django.http import FileResponse, Http404
from django.conf import settings
import re

from django.db.models import Count
from django.core.paginator import Paginator
import json
from functions.contantes import *
from functions.gerador import inicia_gerador

# Create your views here.

# for file in os.listdir("media/viabilidades"):
#     filepath = os.path.join("media/viabilidades", file)
#     estado = filepath.split(".")[0].split("_")[-1]
#     tipo_viabilidade = filepath.split(".")[0].split("_")[-2]

#     quantidade = len(pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL).index)
#     salva_dado(f"Quantidade de Empresas com Viabilidade {tipo_viabilidade} no Estado {estado}", quantidade)

# for file in os.listdir("media/viabilidades_credito"):
#     filepath = os.path.join("media/viabilidades_credito", file)
#     estado = filepath.split(".")[0].split("_")[-1]
#     tipo_viabilidade = filepath.split(".")[0].split("_")[-2]

#     df_viabilidade = pd.read_csv(filepath, sep=";", dtype=DTYPES_RECEITA_FEDERAL)

#     df_meis = df_viabilidade[df_viabilidade["MEINAOMEI"] == "S"]
#     df_N_meis = df_viabilidade[df_viabilidade["MEINAOMEI"] != "S"]
#     salva_dado(
#         f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito aprovado no estado {estado} - MEI", 
#         len(df_meis[df_meis["credito"] == "Aprovado"]["cnpj"].unique().tolist())
#     )
#     salva_dado(
#             f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito aprovado no estado {estado} - NAO MEI", 
#             len(df_N_meis[df_N_meis["credito"] == "Aprovado"]["cnpj"].unique().tolist())
#         )

#     salva_dado(
#             f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito negado no estado {estado} - MEI", 
#             len(df_meis[df_meis["credito"] == "Negado"]["cnpj"].unique().tolist())
#         )
#     salva_dado(
#         f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e crédito negado no estado {estado} - NAO MEI", 
#         len(df_N_meis[df_N_meis["credito"] == "Negado"]["cnpj"].unique().tolist())
#     )
#     salva_dado(
#             f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e sem infos de crédito no estado {estado} - NAO MEI", 
#             len(df_N_meis[df_N_meis["credito"] == "Sem Infos"]["cnpj"].unique().tolist())
#         )

#     salva_dado(
#             f"Quantidade de cnpjs com viabilidade {tipo_viabilidade} e sem infos de crédito no estado {estado} - MEI", 
#             len(df_meis[df_meis["credito"] == "Sem Infos"]["cnpj"].unique().tolist())
#         )
    
# salva_dado("Total Empresas Receita Federal", 27924132)

verifica_atualizacao_receita()

class Dashboard(LoginRequiredMixin,TemplateView):
    template_name = "dashboard.html"
    
    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx["dados"] = DadoExtracao.objects.filter().order_by("-id")
        if not DadoExtracao.objects.filter(titulo="Total Empresas Receita Federal").exists():
            DadoExtracao.objects.create(titulo="Total Empresas Receita Federal", quantidade=27924132)
        ctx["total_empresas"] = DadoExtracao.objects.filter(titulo="Total Empresas Receita Federal").order_by("-id")[0]
        
        total_viabilidades = 0
        total_viabilidades_primarias = 0 
        total_viabilidades_secundarias = 0 

        dados_credito = []

        quantidade_credito_aprovado_N_mei = 0
        quantidade_credito_aprovado_mei = 0
        
        
        quantidade_credito_negado_N_mei = 0
        quantidade_credito_negado_mei = 0
        

        quantidade_credito_sem_info_N_mei = 0
        quantidade_credito_sem_info_mei = 0

        for estado in ESTADOS_BR:
            dado = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de Empresas com Viabilidade Primaria no Estado {estado}").order_by("-id")[0]
            total_viabilidades += dado.quantidade
            total_viabilidades_primarias += dado.quantidade

            dado = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de Empresas com Viabilidade Secundaria no Estado {estado}").order_by("-id")[0]
            total_viabilidades += dado.quantidade
            total_viabilidades_secundarias += dado.quantidade

            dado_NMEI_primario = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Primaria e crédito aprovado no estado {estado} - NAO MEI").order_by("-id")[0]
            quantidade_credito_aprovado_N_mei += dado_NMEI_primario.quantidade
            dado_NMEI_secundaria = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Secundaria e crédito aprovado no estado {estado} - NAO MEI").order_by("-id")[0]
            quantidade_credito_aprovado_N_mei += dado_NMEI_secundaria.quantidade
            dado_MEI_primario = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Primaria e crédito aprovado no estado {estado} - MEI").order_by("-id")[0]
            quantidade_credito_aprovado_mei += dado_MEI_primario.quantidade
            dado_MEI_secundaria = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Secundaria e crédito aprovado no estado {estado} - MEI").order_by("-id")[0]
            quantidade_credito_aprovado_mei += dado_MEI_secundaria.quantidade

            dado_MEI_secundaria_negado = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Secundaria e crédito negado no estado {estado} - MEI").order_by("-id")[0]
            quantidade_credito_negado_mei += dado_MEI_secundaria_negado.quantidade
            dado_MEI_primario_negado = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Primaria e crédito negado no estado {estado} - MEI").order_by("-id")[0]
            quantidade_credito_negado_mei += dado_MEI_primario_negado.quantidade
            dado_NMEI_secundaria_negado = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Secundaria e crédito negado no estado {estado} - NAO MEI").order_by("-id")[0]
            quantidade_credito_negado_N_mei += dado_NMEI_secundaria_negado.quantidade
            dado_NMEI_primario_negado = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Primaria e crédito negado no estado {estado} - NAO MEI").order_by("-id")[0]
            quantidade_credito_negado_N_mei += dado_NMEI_primario_negado.quantidade


            dado_MEI_secundaria_sem_infos = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Secundaria e sem infos de crédito no estado {estado} - MEI").order_by("-id")[0]
            quantidade_credito_sem_info_mei += dado_MEI_secundaria_sem_infos.quantidade
            dado_MEI_primario_sem_info = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Primaria e sem infos de crédito no estado {estado} - MEI").order_by("-id")[0]
            quantidade_credito_sem_info_mei += dado_MEI_primario_sem_info.quantidade
            dado_NMEI_secundaria_sem_info = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Secundaria e sem infos de crédito no estado {estado} - NAO MEI").order_by("-id")[0]
            quantidade_credito_sem_info_N_mei += dado_NMEI_secundaria_sem_info.quantidade
            dado_NMEI_primario_sem_info = DadoExtracao.objects.filter(titulo__icontains=f"Quantidade de cnpjs com viabilidade Primaria e sem infos de crédito no estado {estado} - NAO MEI").order_by("-id")[0]
            quantidade_credito_sem_info_N_mei += dado_NMEI_primario_sem_info.quantidade
        
        ctx["total_empresas_viabilidade"] = total_viabilidades
        ctx["total_empresas_viabilidade_primaria"] = total_viabilidades_primarias
        ctx["total_empresas_viabilidade_secundaria"] = total_viabilidades_secundarias

        ctx["quantidade_credito_aprovado_N_mei"] = quantidade_credito_aprovado_N_mei
        ctx["quantidade_credito_aprovado_mei"] = quantidade_credito_aprovado_mei
        ctx["quantidade_credito_negado_N_mei"] = quantidade_credito_negado_N_mei
        ctx["quantidade_credito_negado_mei"] = quantidade_credito_negado_mei
        ctx["quantidade_credito_sem_info_N_mei"] = quantidade_credito_sem_info_N_mei
        ctx["quantidade_credito_sem_info_mei"] = quantidade_credito_sem_info_mei

        possiveis_status = Status_Execucoe_DB.objects.filter(sistema="geral").order_by("-id")
        if possiveis_status.exists():
            status = possiveis_status[0]
            ctx["ultima_exec"] = status.momento_inicializacao

        return ctx

class Status_Execucao(LoginRequiredMixin,TemplateView):
    template_name = "status_execucao.html"
    def get_context_data(self, **kwargs):
        context =  super().get_context_data(**kwargs)

        sistema = self.request.GET.get("sistema", "geral")
        titulos = {
            'oi': "Mailing Original",
            'geral': "Mailing Original",
            'giga_mais': "Mailing Giga +",
            'janeiro_2026': "Mailing Janeiro 2026"
        }
        context["sistema"] = sistema
        context['titulo'] = titulos[sistema]
        self.request.session["sistema"] = sistema

        context["acompanhar_gerador_activate"] = "active"

        possiveis_status = Status_Execucoe_DB.objects.filter(sistema=sistema).order_by("-id")
        if possiveis_status.exists():
            status = possiveis_status[0]
            context["data_inicializacao"] = status.momento_inicializacao
            context["data_finalizacao"] = status.momento_finalizacao

            context["fases"] = [f for f in Fase_Execucao_DB.objects.filter(status_execucao=status).order_by("-id")]
            for fase in context["fases"]:
                print(fase.titulo)
            context["fase_atual"] = context["fases"][len(context["fases"])-1].titulo
        return context

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
            "Limpeza de BlackList": "Envie aqui um arquivo para que sejam removidos os telefones que estão na BlackList e Quarentena"
        }
        context["descricao"] = dict_tipos[context["tipo_tratamento"]]
        return context
    
    def post(self,request,*args, **kwargs):
        arquivos = request.FILES.getlist('arquivo')

        pasta_usuario = os.path.join(os.getcwd(), "media", f"{request.user.username}")
        pasta_destino = os.path.join( pasta_usuario, "arquivos_externos",)
        zip_path = os.path.join(pasta_usuario,"arquivos_filtragem.zip")
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
            sucesso, mensagem = verifica_arquivo(request,arquivo, destino, "", "")
            if sucesso:
                sucessos.append(mensagem)
            else:
                erros.append(mensagem)
        pasta_raiz = os.path.join(os.getcwd(), "media")
        tipo_tratamento = self.request.GET.get("tipo_tratamento")
        if tipo_tratamento == "Limpeza de BlackList":
            relatorio, erros_internos = filtra_arquivos(pasta_raiz, pasta_destino, pasta_usuario)
        
        if tipo_tratamento == "Limpeza de BlackList":
            relatorio, erros_internos = filtra_arquivos(pasta_raiz, pasta_destino, pasta_usuario)

        relatorio = relatorio.split("\n")
        url_path = reverse('download_arquivo')  # ou reverse('minha_rota', kwargs={'pasta': pasta}) se tiver parâmetros nomeados
        url_relativa = f"{url_path}?full_path={zip_path}"
        url_completa = request.build_absolute_uri(url_relativa)
        

        if erros_internos:
            for er in erros_internos:
                erros.append(er) 

        print(f"Sucessos: {sucessos}")
        print(f"Erros: {erros}")
        print(f"Relatório: {relatorio}")
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

        dict_tipos = {
            "BlackList": "Base de telefones em BlackList que NUNCA devem ser utilizados",
            "Quarentena": "Base de telefones que ficarão em quarentena por determinado período até poderem ser utilizados."
        }
        context["descricao"] = dict_tipos[context["base"]]
        return context

    def post(self,request,*args, **kwargs):
        arquivos = request.FILES.getlist('arquivo')
        base = self.request.GET.get("base")
        PASTAS_RAIZ = {
            "BlackList": "arquivos_blacklist",
            "Quarentena": "arquivos_quarentena",
        }

        pasta_destino = os.path.join(os.getcwd(), "media", PASTAS_RAIZ[base])
        os.makedirs(pasta_destino, exist_ok=True)
        
        if base in ["BlackList", ]:
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
            sucesso, mensagem = verifica_arquivo(request,arquivo, destino, PASTAS_RAIZ[base], "")
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

        ctx = self.get_context_data()
        ctx["show_modal"] = True
        ctx["modal_type"] = "success" if not erros else "error"
        ctx["messages"] = relatorio if not erros else erros
        # ctx["download_url"] = ""
        return render(request, self.template_name, ctx)

def filtra_mailing_view(request):    


    context = {
        'nome_dados': 'Empresas',
        'estados': ESTADOS_BR,
        'resultados': None,
        'qtd_resultados': 0,
        'colunas': [],
        'cnaes': get_cnaes()
    }
    nome_padrao_arquivo = ""
    PASTAS_RAIZ = {
            "oi": os.path.join(os.getcwd(), "media"),
            "geral": os.path.join(os.getcwd(), "media"),
            "giga_mais": os.path.join(os.getcwd(), "media_giga_mais"),
            "janeiro_2026": os.path.join(os.getcwd(), "media_janeiro_2026")
        }
    pasta_raiz = PASTAS_RAIZ[request.session["sistema"]]

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

                return render(request, 'filtros_mailing.html', context)

            filtros["credito"] = tipos_credito
            print("FILTROS: ", filtros)
            pasta_dados = os.path.join(pasta_raiz, "viabilidades_credito_enriquecido")
            df = get_dados_mailing(filtros, tipos_credito=tipos_credito, formato_saida=formato_saida, conjunto_telefones=conjunto_telefones, tipos_telefone= tipos_telefone, tipoMailing=tipoMailing, filtro_telefone_blacklist=filtro_telefone_blacklist, pasta_dados=pasta_dados)
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
            data_atual = f'{dia}-{meses[str(datetime.datetime.now().month)]}'
            nome_padrao_arquivo += data_atual
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

                response = FileResponse(open(f"{pasta_raiz}/{request.user.username}_filtrados_mailing.zip", 'rb'), as_attachment=True, filename='dados_filtrados.zip')
                return response
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

def filtro_geral_view(request):
    context = {
        'nome_dados': 'Empresas',
        'estados_municipios': DICT_ESTADOS_MUNICIPIOS,
        'cnaes': get_cnaes()
    }

    nome_padrao_arquivo = ""
    filepath_csv = os.path.join(os.getcwd(), "media/arquivos_receita_federal_filtrados")
    for file in os.listdir(filepath_csv):
        os.remove(os.path.join(filepath_csv, file))
    if request.method == 'POST':
        try:
            # Processar filtros
            filtros = {}

            # Estados (múltiplos valores via checkbox)
            estados = request.POST.getlist('estado', [])
            print(f"ESTADOS: {estados}")
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
                    nome_padrao_arquivo += f"{estado}_"

            # CNAE (múltiplos valores)
            cnaes = request.POST.getlist('cnae', '')
            print(f"CNAES: {cnaes}")
            if cnaes:
                filtros['cnae_fiscal'] = cnaes

            # Município (múltiplos valores)
            municipios = request.POST.getlist('municipio', '')
            print(f"MUNICIPIOS: {municipios}")
            if municipios:
                filtros['municipio'] = municipios

            # termos_chave = request.POST.get("termos_chave", "")
            # if termos_chave:
            #     filtros["termos_chave"] = termos_chave

            tipo_empresa = request.POST.get("tipo_mei", "")
            print(f"Tipo de empresa: {tipo_empresa}")

            if tipo_empresa:
                filtros["MEINAOMEI"] = tipo_empresa

            # Obter dados do CSV
            df = get_dados_csv(filtros)
            # Converter colunas categóricas para strings
            for col in df.select_dtypes(include=['category']).columns:
                df[col] = df[col].astype(str)



            tipoTelefone = request.POST.get('tipoTelefone', '')
            print(f"Tipo de telefone: {tipoTelefone}")

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
            data_atual = f'{dia}-{meses[str(datetime.datetime.now().month)]}'
            nome_padrao_arquivo += data_atual
            # Preparar dados para exibição
            
            if len(df.index) > 0:
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

                zip_folder(filepath_csv, f"media/{request.user.username}_filtrados.zip")

                context['resultados'] = df.replace({pd.NA: ''}).head(50).values.tolist()
                context['colunas'] = df.columns.tolist()
                context['qtd_resultados'] = len(df.index)

            response = FileResponse(open(f"media/{request.user.username}_filtrados.zip", 'rb'), as_attachment=True, filename='dados_filtro_geral.zip')
            return response
        except Exception as e:
            return JsonResponse({"error": traceback.format_exc()})

    return render(request, 'filtro_geral.html', context)