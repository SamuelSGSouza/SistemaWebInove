from django.urls import path
from . import views

urlpatterns = [
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('', views.Dashboard.as_view(), name='dashboard'),  # Exemplo de rota após login
    path('inicia_gerador', views.IniciaGerador.as_view(), name='inicia_gerador'),  # Exemplo de rota após login
    path('acompanha_gerador', views.AcompanhaGerador.as_view(), name='acompanha_gerador'),  # Exemplo de rota após login
    path('upload_base/', views.upload_base, name='upload_base'),
    path('baixar_pasta_zip/', views.ver_ou_baixar_pasta_zip, name='baixar_pasta_zip'),
    path('dados-grafico/', views.dados_grafico, name='dados_grafico'),
    path('visualizar-dados/', views.visualizar_dados, name='visualizar_dados_filtrados'),
    path('baixar-filtrado/', views.baixar_filtrado, name='baixar_filtrado'),
    path('get_municipios/', views.get_municipios, name='get_municipios'),
    path("dadosddd/", views.dadosddd_dashboard, name="dadosddd_dashboard"),
    path("dados_mailing", views.dados_mailing, name="dados_mailing"),
    path("visualizar_dados_cnpj", views.VisualizarDadosCNPJ.as_view(), name="visualizar_dados_cnpj")
]