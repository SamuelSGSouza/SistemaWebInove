from django.urls import path
from .views import *
urlpatterns = [
    path('', Dashboard.as_view(), name="dashboard"),
    path('status_execucao', Status_Execucao.as_view(), name="status_execucao"),
    path('filtra_mailing', filtra_mailing_view, name="filtra_mailing"),
    path('inicia_gerador_view', inicia_gerador_view, name="inicia_gerador_view"),
]
