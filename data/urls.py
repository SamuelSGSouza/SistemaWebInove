from django.urls import path
from .views import *
urlpatterns = [
    path('', Dashboard.as_view(), name="dashboard"),
    path('status_execucao', Status_Execucao.as_view(), name="status_execucao"),
    path('filtra_mailing', filtra_mailing_view, name="filtra_mailing"),
    path('filtro_geral', filtro_geral_view, name="filtro_geral"),
    path('inicia_gerador_view', inicia_gerador_view, name="inicia_gerador_view"),
    path('download_arquivo', download_arquivo_view, name="download_arquivo"),
    path('atualizacao_bases', AtualizaBases.as_view(), name="atualizacao_bases"),
    path('tratamento_arquivos_externos', TratamentosArquivosExternos.as_view(), name="tratamento_arquivos_externos"),
    path("dashboard/dados/", dados_dashboard, name="dados_dashboard"),
    path("dashboard/dados_tempo/", dados_tempo, name="dados_tempo"),
]
