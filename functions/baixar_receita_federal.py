import requests, os, math, shutil, re,time, zipfile,gc, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from unidecode import unidecode
from data.models import *
from functions.utils import *
import tracemalloc
from functions.contantes import *


COLUNAS_TIPOS_ARQUIVOS = {
    "empresa": ['cnpj_basico','razao_social','natureza_juridica','qualificacao_responsavel','capital_social','porte_empresa','ente_federativo_responsavel',],
    "estabelecimentos": ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia','situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividades', 'cnae_fiscal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro','numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial'],
    "municipios": ['codigo','descricaoM'],
    "naturezajuridica": ['codigo','descricaonj'],
    "socios": ['cnpj_basico','identificador_de_socio','nome_socio','cnpj_cpf_socio','qualificacao_socio','data_entrada_sociedade','pais_socios','representante_legal','nome_representante','qualificacao_representante_legal','faixa_etaria'],
    "simples": ['cnpj_basico','opcao_simples','data_inicio_simples','data_exclusao_simples','opcao_mei','data_inicio_mei','data_exclusao_mei',],
    "cnaes": ['codigo','descricaocf']
}
DTYPES = {
    'empresa':{
        'cnpj_basico': 'string',
        'razao_social': 'string',
        'natureza_juridica': 'category',
        'qualificacao_responsavel': 'category',
        'capital_social': 'string',
        'porte_empresa': 'category',
        'ente_federativo_responsavel': 'category'
    },
    "estabelecimentos": {
        'cnpj_basico': "string", 
        'cnpj_ordem': "category", 
        'cnpj_dv': "category", 
        'matriz_filial': "category",
        'nome_fantasia': "string",
        'situacao_cadastral': "category", 
        'data_situacao_cadastral':"string", 
        'motivo_situacao_cadastral': "category", 
        'nome_cidade_exterior': "string", 
        'pais': "category", 
        'data_inicio_atividades': "string", 
        'cnae_fiscal': "string", 
        'cnae_fiscal_secundaria':"string", 
        'tipo_logradouro': "category", 
        'logradouro': "string",
        'numero': "string", 
        'complemento': "string", 
        'bairro': "string", 
        'cep': "string", 
        'uf': "category", 
        'municipio': "category", 
        'ddd1': "category", 
        'telefone1': "string", 
        'ddd2': "category", 
        'telefone2': "string", 
        'ddd_fax': "category", 
        'fax': "string", 
        'correio_eletronico': "string", 
        'situacao_especial': "string", 
        'data_situacao_especial': "string"
    },
    "socios": {
        "cnpj_basico": "string",
        "identificador_de_socio": "category",
        "nome_socio": "string",
        'cnpj_cpf_socio': "string",
        'qualificacao_socio': "category",
        'data_entrada_sociedade':"string",
        'pais_socios': "category",
        'representante_legal': "category",
        'nome_representante': "string",
        'qualificacao_representante_legal': "category",
        'faixa_etaria': "category"
    },
    "cnaes": "string",
    "municipios": "string",
    "naturezajuridica": "string",
    "simples": {
        'cnpj_basico': "string",
        'opcao_simples': "category",
        'data_inicio_simples': "category",
        'data_exclusao_simples': "category",
        'opcao_mei': "category",
        'data_inicio_mei': "category",
        'data_exclusao_mei': "category",
    }
}


lista_possiveis_enumeracoes_tipologradouro = [
    'PRIMEIRA', 'SEGUNDA', 'TERCEIRA', '1A', '1', '2', '2A', '3', '3A'
]
lista_col = ['data_inicio_atividades' , 'natureza_juridica' , 'descricaonj' , 'cnae_fiscal', 'cnae_fiscal_secundaria', 'descricaocf' , 'cnpj' , 'razao_social', 'nome_fantasia', 'matriz_filial', 'decisor' , 'situacao_cadastral', 'correio_eletronico' , 'logradouro' , 'num_fachada' , 'complemento1' , 'bairro' , 'cep' , 'municipio' , 'uf', 'CPF', 'MEINAOMEI', 'TEL1' , 'TEL2' , 'TEL3']


pasta_destino = os.path.join(os.getcwd(), "media/arquivos_receita_federal")
os.makedirs(pasta_destino, exist_ok=True)
total_dados = 0
total_dados_receita_Mei = 0
def salva_log_geral(msg:str, sistema:str="geral"):
    salva_log(msg, sistema)

def baixa_arquivos_receita():
    salva_log_geral("Iniciou Exclusão de dados anteriores da Receita Federal")

    for item in os.listdir(pasta_destino):
        item_full_path = os.path.join(pasta_destino, item)
        if os.path.isfile(item_full_path) or os.path.islink(item_full_path):
            os.remove(item_full_path)
        elif os.path.isdir(item_full_path):
            shutil.rmtree(item_full_path)
    salva_log_geral("Finalizou Exclusão de dados anteriores da Receita Federal")

    # Calcula a URL com base no mês anterior
    hoje = datetime.today()
    mes_passado = hoje - relativedelta(months=0)
    data_formatada = mes_passado.strftime("%Y-%m")
    url_base = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{data_formatada}/Cnaes.zip"
    file_url = ""
    resposta_teste = requests.get(url_base)
    if str(resposta_teste.status_code) == "404":
        mes_passado = hoje - relativedelta(months=1)
        data_formatada = mes_passado.strftime("%Y-%m")
    
    files_to_download = [
        "Cnaes.zip",
        "Empresas0.zip",
        "Empresas1.zip",
        "Empresas2.zip",
        "Empresas3.zip",
        "Empresas4.zip",
        "Empresas5.zip",
        "Empresas6.zip",
        "Empresas7.zip",
        "Empresas8.zip",
        "Empresas9.zip",
        "Estabelecimentos0.zip",
        "Estabelecimentos1.zip",
        "Estabelecimentos2.zip",
        "Estabelecimentos3.zip",
        "Estabelecimentos4.zip",
        "Estabelecimentos5.zip",
        "Estabelecimentos6.zip",
        "Estabelecimentos7.zip",
        "Estabelecimentos8.zip",
        "Estabelecimentos9.zip",
        "Motivos.zip",
        "Municipios.zip",
        "Naturezas.zip",
        "Paises.zip",
        "Qualificacoes.zip",
        "Simples.zip",
        "Socios0.zip",
        "Socios1.zip",
        "Socios2.zip",
        "Socios3.zip",
        "Socios4.zip",
        "Socios5.zip",
        "Socios6.zip",
        "Socios7.zip",
        "Socios8.zip",
        "Socios9.zip",
    ]

    try:
        for file_name in files_to_download:
            file_url = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{data_formatada}/{file_name}"

            with requests.get(file_url, stream=True, timeout=300) as r:
                r.raise_for_status()
                file_path = os.path.join(pasta_destino, file_name)
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        

    except Exception as e:
        salva_log_geral(f"Erro ao baixar o arquivo com url : {file_url}. Erro: {e}")

def extrair_zip_e_renomear(pasta_destino=pasta_destino):
    salva_log_geral(f"Iniciou processo de extração dos arquivos zipados da Receita Federal")
    dados = [
        {"prefixo": "Cnaes", "pasta": "cnaes", "novo_nome": "cnaes.csv"},
        {"prefixo": "Motivos", "pasta": "motivos", "novo_nome": "motivos.csv"},
        {"prefixo": "Municipios", "pasta": "municipios", "novo_nome": "municipios.csv"},
        {"prefixo": "Naturezas", "pasta": "naturezajuridica", "novo_nome": "naturezajuridica.csv"},
        {"prefixo": "Paises", "pasta": "paises", "novo_nome": "paises.csv"},
        {"prefixo": "Qualificacoes", "pasta": "qualificacoesdossocios", "novo_nome": "qualificacoesdossocios.csv"},
        {"prefixo": "Simples", "pasta": "simples", "novo_nome": "simples.csv"}
    ]

    # Adiciona entradas numéricas de forma dinâmica
    for tipo in ["Empresas", "Estabelecimentos", "Socios"]:
        for i in range(10):
            dados.append({
                "prefixo": f"{tipo}{i}",
                "pasta": tipo.lower() if tipo != "Empresas" else "empresa",
                "novo_nome": f"{tipo.lower() if tipo != 'Empresas' else 'empresa'}{i}.csv"
            })

    for item in dados:
        # Cria objetos Path para manipulação de caminhos
        zip_path = Path(pasta_destino) / f"{item['prefixo']}.zip"
        dest_dir = Path(pasta_destino) / item['pasta']
        novo_nome = item['novo_nome']

        try:
            # Cria o diretório se não existir
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extrai todos os arquivos
                zip_ref.extractall(dest_dir)
                
                # Renomeia o primeiro arquivo do ZIP (assumindo que há apenas um)
                arquivo_extraido = zip_ref.namelist()[0]
                (dest_dir / arquivo_extraido).rename(dest_dir / novo_nome)
            salva_log_geral(f"Extraiu com sucesso o arquivo {zip_path}")
            os.remove(zip_path)      
        except FileNotFoundError:
            salva_log_geral(f"Arquivo ZIP não encontrado: {zip_path}")
        except IndexError:
            salva_log_geral(f"ZIP vazio ou inválido: {zip_path}")
        except Exception as e:
            salva_log_geral(f"Erro ao processar {zip_path}: {str(e)}")

def unifica_dados(nova_execucao):
    
    ##FUNÇÕES DE APOIO NA GERAÇÃO DE DADOS
    def fillna_categoricals(df, fill_value=''):
        for col in df.select_dtypes(include='category').columns:
            if fill_value not in df[col].cat.categories:
                df[col] = df[col].cat.add_categories([fill_value])
            df[col] = df[col].fillna(fill_value)
        for col in df.select_dtypes(exclude='category').columns:
            df[col] = df[col].fillna(fill_value)
        return df

    def tratar_string(x):
        x = unidecode(x) 
        x = x.upper()  
        x = re.sub(r'\s+', ' ', x)
        x = re.sub(r'^[^A-Z0-9]+', '', x)
        return x

    def criar_cnpj(row):
        cnpj_basico = row['cnpj_basico']
        cnpj_ordem = row['cnpj_ordem']
        cnpj_dv = row['cnpj_dv']

        return str(cnpj_basico) + str(cnpj_ordem) + str(cnpj_dv)

    def criar_tel1(row):
        ddd = row['ddd1']
        numero = row['telefone1']
        try:
            ddd = int(ddd)
            if ddd == 0:
                ddd = ''
            numero = int(numero)
        except:
            ddd = str(ddd)
            if ddd == '0':
                ddd = ''
            numero = str(numero)
            
        tel = str(ddd) + str(numero)
        return tel

    def criar_tel2(row):
        ddd = row['ddd2']
        numero = row['telefone2']
        try:
            ddd = int(ddd)
            if ddd == 0:
                ddd = ''
            numero = int(numero)
        except:
            ddd = str(ddd)
            if ddd == '0':
                ddd = ''
            numero = str(numero)
            
        tel = str(ddd) + str(numero)
        return tel

    def criar_tel_fax(row):
        ddd = row['ddd_fax']
        numero = row['fax']
        try:
            ddd = int(ddd)
            if ddd == 0:
                ddd = ''
            numero = int(numero)
        except:
            ddd = str(ddd)
            if ddd == '0':
                ddd = ''
            numero = str(numero)
            
        tel = str(ddd) + str(numero)
        return tel

    def criar_decisor(row):
        socio = str(row['nome_socio'])
        representante = str(row['nome_representante'])
        razao_social = str(row['razao_social'])
        
        if socio != "" and representante != "":
            return socio
        if socio == "" and representante != "":
            return representante
        else:
            comeco = str(razao_social).split(' ')[0].replace('.', '')
            
            try:
                _ = str(int(comeco))
            except:
                return ''

            if len(comeco) < 8: return ''

            return ' '.join(str(razao_social).split(' ')[1:])

    def verificar_digitos(nome):
        if pd.isna(nome) or not isinstance(nome, str):
            return False
        palavras = nome.split()
        if not palavras:
            return False
        primeira_palavra = re.sub(r'\D', '', palavras[0])
        ultima_palavra = re.sub(r'\D', '', palavras[-1])
        return len(primeira_palavra) >= 7 or len(ultima_palavra) >= 7

    def ajustar_meinmei(linha):
        if 'LTDA' in linha['razao_social']:
            return 'N'
        elif 'LTDA' in linha['decisor']:
            return 'N'
        elif linha['natureza_juridica'] == '2135':
            return 'S'
        elif verificar_digitos(linha['razao_social']):
            return 'S'
        elif verificar_digitos(linha['decisor']):
            return 'S'
        else:
            return linha['MEINAOMEI']

    def tratamentosBAIRRO(texto):
        texto = str(texto)
        if texto == "":
            texto = "0"
        texto = " ".join(texto.split())
        if "(" in texto:
            texto = texto.split("(")[0]
        elif "," in texto:
            texto = texto.split(",")[0]
        texto = " ".join(texto.split())
        return texto

    def tratar_logradouro(row):


        def _tratar_tl(tipo_logradouro):

            if '' == tipo_logradouro or tipo_logradouro == '0':
                return 'RUA'
            elif "AVEN" in tipo_logradouro:
                return "AVENIDA"
            elif "ANEL" in tipo_logradouro:
                return "ANEL"
            elif "ROD" in tipo_logradouro:
                return "RODOVIA"
            elif "R" in tipo_logradouro or 'R.' in tipo_logradouro:
                return "RUA"
            elif "ESTRADA" in tipo_logradouro:
                return "ESTRADA"
            elif "TRAV" in tipo_logradouro:
                return "TRAVESSA"
            elif "COMPL" in tipo_logradouro or "COMP." in tipo_logradouro or "COMPLEXO" in tipo_logradouro:
                return "COMPLEXO"
            elif "ESC" in tipo_logradouro or "ESCADA" in tipo_logradouro:
                return "ESCADA"
            elif "SUB" in tipo_logradouro:
                return "SUBIDA"
            elif "VIL" in tipo_logradouro:
                return "VILA"
            elif "BAL" in tipo_logradouro:
                return "BALAO"
            elif "BEC" in tipo_logradouro:
                return "BECO"
            elif "ALT" in tipo_logradouro:
                return "ALTO"
            elif "PARQ" in tipo_logradouro:
                return "PARQUE"
            elif "LADEI" in tipo_logradouro:
                return "LADEIRA"
            elif "ALAM" in tipo_logradouro or "EST." in tipo_logradouro or "ESTR" in tipo_logradouro:
                return "ALAMEDA"
            elif "ÁREA" in tipo_logradouro or "AREA" in tipo_logradouro:
                return "AREA"
            elif "ACES" in tipo_logradouro:
                return "ACESSO"
            elif "AV." in tipo_logradouro:
                return "AVENIDA"
            elif "CONJ" in tipo_logradouro:
                return "CONJUNTO"
            elif "COND" in tipo_logradouro:
                return "CONDOMINIO"
            elif "ENTR" in tipo_logradouro:
                return "ENTRADA"
            elif "ES." in tipo_logradouro or "ESPA" in tipo_logradouro:
                return "ESTRADA"
            elif "VIA" in tipo_logradouro:
                return "VIA"
            elif "RESIDENCIAL" in tipo_logradouro:
                return "RESIDENCIAL"
            elif "GALERIA" in tipo_logradouro:
                return "GALERIA"
            elif "LARGO" in tipo_logradouro:
                return "LARGO"
            elif "BOUL" in tipo_logradouro:
                return "BOULEVARD"
            elif "FAZ" in tipo_logradouro:
                return "FAZENDA"
            elif "LOTEAMENTO" in tipo_logradouro:
                return "LOTEAMENTO"
            elif "OUT" in tipo_logradouro:
                return "OUTROS"
            elif "PRA" in tipo_logradouro:
                return "PRACA"
            elif "PS" in tipo_logradouro:
                return "PASSARELA"
            elif "RODO" in tipo_logradouro:
                return "RODOVIA"
            elif "CAMI" in tipo_logradouro:
                return "CAMINHO"
            elif "ESTA" in tipo_logradouro:
                return "ESTACAO"
            elif "JARD" in tipo_logradouro:
                return "JARDIM"
            elif "MOD" in tipo_logradouro:
                return "MODULO"
            elif "NUCL" in tipo_logradouro or "NÚCL" in tipo_logradouro:
                return "NUCLEO"
            elif "PASSE" in tipo_logradouro:
                return "PASSEIO"
            elif "SERVID" in tipo_logradouro:
                return "SERVIDAO"
            elif "PISTA" in tipo_logradouro:
                return "PISTA"
            elif "PARAL" in tipo_logradouro:
                return "PARALELA"
            elif "EIXO" in tipo_logradouro:
                return "EIXO"
            elif "VIELA" in tipo_logradouro:
                return "VIELA"
            elif "TV" in tipo_logradouro:
                return "TRAVESSA"
            elif "PQ" in tipo_logradouro:
                return "PARQUE"

            if ' ' in tipo_logradouro:
                ver_tipo_logradouro = str(tipo_logradouro).split(' ')
                if len(ver_tipo_logradouro) == 2 and ver_tipo_logradouro[0] in lista_possiveis_enumeracoes_tipologradouro:
                    return ver_tipo_logradouro[1]

            return tipo_logradouro

        def _tratar_l(logradouro):

            ver_logradouro = str(logradouro).split(' ')
            if final_tl == ver_logradouro[0]:
                return " ".join(ver_logradouro)

            return logradouro

        tipo_logradouro = row['tipo_logradouro']
        logradouro = row['logradouro']

        final_tl = _tratar_tl(tipo_logradouro)
        final_l = _tratar_l(logradouro)

        final = final_tl + " " + final_l

        return final

    def extrair_cpf(row) -> str:

        nome = row['razao_social']
        decisor = row['decisor']
        ultima_palavra_nome = str(nome).split(' ')[-1].replace('-', '').replace('.', '').replace('/', '')
        ultima_palavra_decisor = str(decisor).split(' ')[-1].replace('-', '').replace('.', '').replace('/', '')
        if ultima_palavra_decisor.isnumeric():
            if len(ultima_palavra_decisor) == 11:
                return ultima_palavra_decisor
        elif ultima_palavra_nome.isnumeric():
            if len(ultima_palavra_nome) == 11:
                return ultima_palavra_nome
        return ''

    def criar_dataframe(file_path, campos_tabela, dtypes=str, colunas_necessarias=[]):
        if not colunas_necessarias:
            colunas_necessarias = campos_tabela
        folder_path = os.path.join(pasta_destino, file_path)
        df_list = []
        # dtypes = DTYPES.get("")
        for file in os.listdir(folder_path):
            csv = os.path.join(folder_path, file)
            df = pd.read_csv(csv, sep = ';', names = campos_tabela, encoding = 'latin-1',usecols=colunas_necessarias, dtype = dtypes, skipinitialspace=True, quotechar='"')
            df = fillna_categoricals(df)
            df_list.append(df)
            salva_log_geral(f"leu arquivo: {csv}")
        df_concat = pd.concat(df_list)
        del df_list
        return df_concat
    

    #FIM DAS FUNÇÕES DE APOIO
    df_empre = criar_dataframe("empresa", COLUNAS_TIPOS_ARQUIVOS["empresa"], DTYPES["empresa"],['cnpj_basico','razao_social','natureza_juridica',])
    df_empre['razao_social'] = df_empre['razao_social'].apply(tratar_string)
    df_empre["cnpj_basico"] = df_empre["cnpj_basico"].astype("string")
    memoria_consumida = df_empre.memory_usage(deep=True).sum() / (1024 * 1024)
    salva_log_geral(f"EMPRESAS CONFIGURADO! Total de registros: {len(df_empre)} Total de memória consumida: {int(memoria_consumida)} MB")
    #EMPRESAS CONFIGURADO! Total de registros: 63235730 Total de memória consumida: 13612 MB

    df_simples = criar_dataframe("simples", COLUNAS_TIPOS_ARQUIVOS["simples"], DTYPES["simples"],['cnpj_basico','opcao_simples','opcao_mei',])
    df_simples["cnpj_basico"] = df_simples["cnpj_basico"].astype("string")
    memoria_consumida = df_simples.memory_usage(deep=True).sum() / (1024 * 1024)
    salva_log_geral(f"SIMPLES CONFIGURADO! Total de registros: {len(df_simples)} Total de memória consumida: {int(memoria_consumida)} MB")

    df_nat = criar_dataframe("naturezajuridica", COLUNAS_TIPOS_ARQUIVOS["naturezajuridica"])
    df_nat['descricaonj'] = df_nat['descricaonj'].apply(tratar_string)
    df_nat["codigo"] = df_nat["codigo"].astype("string")
    df_nat = fillna_categoricals(df_nat)
    memoria_consumida = df_nat.memory_usage(deep=True).sum() / (1024 * 1024)
    salva_log_geral(f"NATUREZA_JURIDICA CONFIGURADO! Total de registros: {len(df_nat)} Total de memória consumida: {int(memoria_consumida)} MB")

    df_cnae = criar_dataframe("cnaes", COLUNAS_TIPOS_ARQUIVOS["cnaes"],DTYPES["cnaes"])
    df_cnae = fillna_categoricals(df_cnae)
    df_cnae["codigo"] = df_cnae["codigo"].astype("string")
    df_cnae['descricaocf'] = df_cnae['descricaocf'].apply(tratar_string)
    memoria_consumida = df_cnae.memory_usage(deep=True).sum() / (1024 * 1024)
    salva_log_geral(f"CNAE CONFIGURADO! Total de registros: {len(df_cnae)} Total de memória consumida: {int(memoria_consumida)} MB")

    df_mun = criar_dataframe("municipios", COLUNAS_TIPOS_ARQUIVOS["municipios"], DTYPES["municipios"])
    df_mun = fillna_categoricals(df_mun)
    memoria_consumida = df_mun.memory_usage(deep=True).sum() / (1024 * 1024)
    salva_log_geral(f"MUNICIPIOS CONFIGURADO! Total de registros: {len(df_mun)} Total de memória consumida: {int(memoria_consumida)} MB")

    df_socio = criar_dataframe("socios", COLUNAS_TIPOS_ARQUIVOS["socios"],DTYPES["socios"],['cnpj_basico','identificador_de_socio','nome_socio','cnpj_cpf_socio','qualificacao_socio','faixa_etaria', "nome_representante"])
    df_socio['nome_socio'] = df_socio['nome_socio'].apply(tratar_string)
    df_socio['nome_representante'] = df_socio['nome_representante'].apply(tratar_string)
    memoria_consumida = df_socio.memory_usage(deep=True).sum() / (1024 * 1024)
    salva_log_geral(f"SOCIOS CONFIGURADO! Total de registros: {len(df_socio)} Total de memória consumida: {int(memoria_consumida)} MB")

    pasta_estabelecimentos = os.path.join(pasta_destino, "estabelecimentos")

    cnpjs_usados = []
    i = 1
    for file in os.listdir(pasta_estabelecimentos):
        csv = os.path.join(pasta_estabelecimentos, file)
        chunks = pd.read_csv(csv, sep = ';', names = COLUNAS_TIPOS_ARQUIVOS["estabelecimentos"], usecols=['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia','situacao_cadastral', 'data_situacao_cadastral', 'pais', 'data_inicio_atividades', 'cnae_fiscal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro','numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico',], encoding = 'latin-1', dtype = DTYPES["estabelecimentos"], skipinitialspace=True, quotechar='"', chunksize=3_000_000)
        for df_estab in chunks:
            df_estab = df_estab[df_estab['situacao_cadastral'].isin(['2', '02'])]
            df_estab = fillna_categoricals(df_estab)

            #CONCATENANDO COM EMPRESAS
            print("formato dados:", df_empre["cnpj_basico"].head())
            print("formato dados:", df_estab["cnpj_basico"].head())
            df_estab_filtr = df_estab[df_estab["cnpj_basico"].isin(df_empre["cnpj_basico"].unique().tolist())]
            print(f"Um total de {len(df_estab_filtr['cnpj_basico'].unique().tolist())}/{len(df_estab['cnpj_basico'].unique().tolist())} possuem infos nas empresas")
            

            df_unificado = pd.merge(df_estab, df_empre, on='cnpj_basico', how='left')
            df_unificado = fillna_categoricals(df_unificado)
            df_unificado["razao_social_na"] = df_unificado["razao_social"].isna()
            df_unificado = df_unificado.sort_values(by=["cnpj_basico", "razao_social_na"])
            df_unificado = df_unificado.drop_duplicates(subset=['cnpj_basico'], keep='first')
            df_unificado = df_unificado.drop(columns=["razao_social_na"])
            salva_log_geral(f"Unificou base de estabelecimentos com a base de empresas através do campo -cnpj_basico-")

            
            #CONCATENANDO COM SIMPLES
            df_unificado = pd.merge(df_unificado, df_simples, left_on='cnpj_basico', right_on='cnpj_basico', how='left')
            df_unificado = fillna_categoricals(df_unificado)
            salva_log_geral(f"Unificou base resultante com a base do simples através do campo -cnpj_basico-")

            #CONCATENANDO COM NATUREZA JURÍDICA
            
            df_unificado["natureza_juridica"] = df_unificado["natureza_juridica"].astype("string")
            df_unificado = pd.merge(df_unificado, df_nat, left_on='natureza_juridica', right_on='codigo', how='left')
            df_unificado = fillna_categoricals(df_unificado)
            df_unificado = df_unificado.drop(columns=['codigo'])
            salva_log_geral(f"Unificou base resultante com a base de natureza jurídica através do campo -natureza_juridica-")

            #CONCATENANDO COM CNAE
            df_unificado["cnae_fiscal"] = df_unificado["cnae_fiscal"].astype("string")
            df_unificado = pd.merge(df_unificado, df_cnae, left_on='cnae_fiscal', right_on='codigo', how='left')
            df_unificado = fillna_categoricals(df_unificado)
            df_unificado = df_unificado.drop(columns=['codigo'])
            salva_log_geral(f"Unificou base resultante com a base de cnaes através do campo -cnae_fiscal-")

            #CONCATENANDO COM MUNICÍPIOS
            df_mun["codigo"] = df_mun["codigo"].astype("string")
            df_unificado["municipio"] = df_unificado["municipio"].astype("string")

            df_unificado = pd.merge(df_unificado, df_mun, left_on='municipio', right_on='codigo', how='left')
            df_unificado = fillna_categoricals(df_unificado)
            df_unificado = df_unificado.drop(columns=['codigo', 'municipio'])
            salva_log_geral(f"Unificou base resultante com a base de municípios através do campo -municipio-")

            #CONCATENANDO COM SÓCIOS
            df_socio["cnpj_basico"] = df_socio["cnpj_basico"].astype("string")
            df_unificado["cnpj_basico"] = df_unificado["cnpj_basico"].astype("string")

        
            df_unificado = pd.merge(df_unificado, df_socio, on='cnpj_basico', how='left')
            df_unificado = fillna_categoricals(df_unificado)
            df_unificado = df_unificado.sort_values(by=['cnpj_basico', 'razao_social'])
            df_unificado = df_unificado.drop_duplicates(subset=['cnpj_basico', 'razao_social'], keep='first')
            salva_log_geral(f"Unificou base resultante com a base de sócios através do campo -cnpj_basico-")
            df_unificado['cnpj'] = df_unificado.apply(criar_cnpj, axis = 1)
            salva_log_geral(f"Criou campo de Cnpj somando os campos  -cnpj_basico- + -cnpj_ordem- + -cnpj_dv-")

            df_unificado['TEL1'] = df_unificado.apply(criar_tel1, axis = 1)
            df_unificado['TEL2'] = df_unificado.apply(criar_tel2, axis = 1)
            df_unificado['TEL3'] = df_unificado.apply(criar_tel_fax, axis = 1)
            df_unificado['decisor'] = df_unificado.apply(criar_decisor, axis = 1)
            salva_log_geral(f"Gerou os telefones 1, 2 e 3(fax)")

            df_unificado['bairro'] = df_unificado['bairro'].apply(tratamentosBAIRRO)
            df_unificado['log'] = df_unificado.apply(tratar_logradouro, axis = 1)
            df_unificado = df_unificado.drop(columns=['tipo_logradouro', 'logradouro'])
            salva_log_geral(f"Tratou os Bairros e Logradouros")


            df_unificado['CPF'] = df_unificado.apply(extrair_cpf, axis=1)

            df_unificado = df_unificado.rename(
                columns = {
                    'descricaoM': 'municipio',
                    'numero' : 'num_fachada', 
                    'complemento' : 'complemento1', 
                    'log': 'logradouro',
                    'opcao_mei': 'MEINAOMEI'
                }
            )

            df_unificado["matriz_filial"] = df_unificado["matriz_filial"].apply(lambda x: 'FILIAL' if "2" in x else "MATRIZ" if "1" in x else "")

            df_unificado = df_unificado[lista_col]
            df_unificado['MEINAOMEI'] = df_unificado.apply(ajustar_meinmei, axis=1)
            
            df_unificado['TEL1'] = df_unificado['TEL1'].apply(lambda x: clean_phone_number(x))
            df_unificado['TEL2'] = df_unificado['TEL2'].apply(lambda x: clean_phone_number(x))
            df_unificado['TEL3'] = df_unificado['TEL3'].apply(lambda x: clean_phone_number(x))

            df_unificado.dropna(subset=["uf",], inplace=True)
            df_unificado["uf"] = df_unificado["uf"].apply(lambda x: x if x != "EX" else "ES")
            
            
            df_unificado["cnpj"] = df_unificado["cnpj"].str.replace(
                r"(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})",
                r"\1.\2.\3/\4-\5",
                regex=True
            )

            df_unificado = df_unificado[~df_unificado["cnpj"].isin(cnpjs_usados)]
            cnpjs_usados += df_unificado["cnpj"].unique().tolist()
            # Agrupa por estado e salva em arquivos separados
            for uf in df_unificado["uf"].unique().tolist():
                nome_arquivo_uf = os.path.join(pasta_destino, f"{uf}.csv")
                df_uf = df_unificado[df_unificado["uf"] == uf]
                # Adiciona ao arquivo do estado, criando ou acrescentando sem cabeçalho
                write_header = not os.path.exists(nome_arquivo_uf)
                df_uf.to_csv(nome_arquivo_uf, mode="a", header=write_header, index=False, sep=";", encoding="utf-8")
                del df_uf
                
            i += 1
            global total_dados
            total_dados += len(df_unificado.index)

            global total_dados_receita_Mei
            df_unificado_meis = df_unificado[df_unificado["MEINAOMEI"] == "S"]
            total_dados_receita_Mei += len(df_unificado_meis.index)

            


            del df_unificado
            del df_unificado_meis
            del df_estab
            gc.collect()

            salva_log_geral(f"Gerou base {i} da receita")
            salva_status(nova_execucao, titulo=f"Gerou base {i} da receita",status="Em Andamento")

        try:
            os.remove(csv)
        except:
            pass
        
    del df_empre
    del df_simples
    del df_nat
    del df_cnae
    del df_socio
    del df_mun
    

    dfs_cluster_1 = []
    dfs_cluster_2 = []
    dfs_cluster_3 = []
    dfs_cluster_4 = []
    for file in os.listdir(pasta_destino):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(pasta_destino,file), sep=";", dtype=DTYPES_RECEITA_FEDERAL)
            df.drop_duplicates(subset=["cnpj",], inplace=True)
            estado = pasta_destino.split(".")[0].split("_")[-1]
            salva_dado(f"Total Empresas ATIVAS no estado {estado}", len(df.index))

            df.to_csv(os.path.join(pasta_destino,file), sep=";",index=False)
            for uf, df_uf in df.groupby("uf"):

                df_uf = df_uf[["cnpj", "uf"]]
                if uf in ["RJ","MG","RS","PR"]:
                    dfs_cluster_1.append(df_uf)
                elif uf in ["ES","PA","BA","DF","AM"]:
                    dfs_cluster_2.append(df_uf)
                elif uf in ["RO", "SC", "MS", "MT", "RR", "GO"]:
                    dfs_cluster_3.append(df_uf)
                elif uf in ["MA", "AC", "AP", "CE", "PE"]:
                    dfs_cluster_4.append(df_uf)
            del df
            salva_log_geral(f"Analisou {file}")

    PASTA_CNPJS_FORMATO_OI = os.path.join(os.getcwd(), "media/arquivos_formato_oi")
    df_cluster_1 = pd.concat(dfs_cluster_1).drop_duplicates(subset=["cnpj",])
    df_cluster_1.to_csv(os.path.join(PASTA_CNPJS_FORMATO_OI, "cluster_1.csv"), sep=";", index=False)
    del df_cluster_1

    df_cluster_2 = pd.concat(dfs_cluster_2).drop_duplicates(subset=["cnpj",])
    df_cluster_2.to_csv(os.path.join(PASTA_CNPJS_FORMATO_OI, "cluster_2.csv"), sep=";", index=False)
    del df_cluster_2

    df_cluster_3 = pd.concat(dfs_cluster_3).drop_duplicates(subset=["cnpj",])
    df_cluster_3.to_csv(os.path.join(PASTA_CNPJS_FORMATO_OI, "cluster_3.csv"), sep=";", index=False)
    del df_cluster_3

    df_cluster_4 = pd.concat(dfs_cluster_4).drop_duplicates(subset=["cnpj",])
    df_cluster_4.to_csv(os.path.join(PASTA_CNPJS_FORMATO_OI, "cluster_4.csv"), sep=";", index=False)
    del df_cluster_4

    zip_folder(PASTA_CNPJS_FORMATO_OI, "media/arquivos_formato_oi.zip")
    
    filepath = os.path.join(os.getcwd(), 'media/arquivos_receita_federal')
    dtypes = {
            'uf': "category",
            'municipio': "category",
            'cnae_fiscal': 'string',
            'bairro': 'category',
            'natureza_juridica': 'category',
            'situacao_cadastral': "category",
            'MEINAOMEI': "category",
        }
    dfs = []
    for f in os.listdir(filepath):
        df_path = os.path.join(filepath, f)
        if f.endswith(".csv"):
            for chunk in pd.read_csv(df_path, sep=";", usecols=["descricaocf", "cnae_fiscal"], chunksize=1_000_000, dtype=dtypes):
                chunk = chunk.drop_duplicates(subset=["cnae_fiscal"]).dropna(subset=["cnae_fiscal","descricaocf"])
                chunk["cnae_desc"] = chunk["cnae_fiscal"] + " - " + chunk["descricaocf"]
                print(chunk)
                dfs.append(chunk)

    dfs = pd.concat(dfs).drop_duplicates(subset=["cnae_desc"])
    dfs.to_csv(os.path.join(os.getcwd(), 'media/dados_cnaes.csv'), sep=";", index=False)
    del dfs

def realiza_limpeza():
    def limpa_pastas(pastas:list)->str:
        for nome_pasta in pastas:
            pasta = os.path.join(pasta_destino, nome_pasta)
            if os.path.exists(pasta):
                shutil.rmtree(pasta)

    limpa_pastas(["cnaes", "empresa", "estabelecimentos", "motivos", "municipios", "naturezajuridica", "paises", "qualificacoesdossocios", "simples", "socios"])

    for arquivo in os.listdir(pasta_destino):
        if arquivo.endswith(".csv") and arquivo.startswith("Base_Receita"):
            os.remove(os.path.join(pasta_destino, arquivo))

@fecha_conexoes
def fase_1_gerador():

    nova_execucao = Status_Execucoe_DB.objects.create(sistema="geral")
    try:
        Log.objects.filter().delete()

        salva_log_geral("Iniciou Sistema de Geração de Mailings")
        
        salva_status(nova_execucao, titulo="Iniciando Download Arquivos da Receita Federal",status="Em Andamento")
        baixa_arquivos_receita()
        
        salva_status(nova_execucao, titulo="Iniciando Extração dos Arquivos da Receita Federal",status="Em Andamento")



        extrair_zip_e_renomear()

        salva_status(nova_execucao, titulo="Iniciando Unificação e limpeza dos Arquivos da Receita Federal",status="Em Andamento")


        unifica_dados(nova_execucao)
        realiza_limpeza()
        salva_status(nova_execucao, titulo="Finalização dos Dados da Receita Federal",status="Concluido")
        print("Começando a verificar")

        if verificador_fase_1(nova_execucao):

            

            global total_dados
            global total_dados_receita_Mei
            salva_dado("Total Empresas Receita Federal", total_dados)
            salva_dado("Total Empresas MEI na Receita Federal", total_dados_receita_Mei)
            salva_dado("Total Empresas NMEI na Receita Federal", total_dados-total_dados_receita_Mei)
            salva_status(nova_execucao, titulo="Dados da Receita Salvos com sucesso.",status="Concluido")
            return True

    except Exception as e:
        salva_status(nova_execucao, titulo=f"Falha ao Tratar dados da Receita Federal {e}",status="Erro")

        return False
    finally:

        zip_folder(pasta_destino, "media/arquivos_receita_federal.zip")
        gc.collect()
        
        return

def verificador_fase_1(nova_execucao):
    estados = [ 'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    #verificar todos os estados foram atualizados na data atual
    root = os.path.join(os.getcwd(), "media", "arquivos_receita_federal")
    colunas_esperadas = ["data_inicio_atividades", "natureza_juridica", "descricaonj", "cnae_fiscal", "cnae_fiscal_secundaria", "descricaocf", "cnpj", "razao_social", "nome_fantasia", "matriz_filial", "decisor", "situacao_cadastral", "correio_eletronico", "logradouro", "num_fachada", "complemento1", "bairro", "cep", "municipio", "uf", "CPF", "MEINAOMEI", "TEL1", "TEL2", "TEL3"]
    cnpjs_encontrados = []
    telefones_encontrados = []


    for estado in estados:
        salva_status(nova_execucao, titulo=f"Verificando integridade dos dados do estado {estado}",status="Em Andamento")

        file = f"{estado}.csv"
        filepath = os.path.join(root,file)
        arquivo = Path(filepath)
        timestamp = arquivo.stat().st_ctime
        data = datetime.fromtimestamp(timestamp)
        hoje = datetime.today()
        if hoje.day != data.day or hoje.month != data.month:
            #data de criação não foi hoje
            salva_status(nova_execucao, titulo=f"Erro ao Tratar Base da Receita: Arquivo {file} não foi criado hoje.",status="Erro")
            return False
        
        #verificar se todos os estados possuem as mesmas colunas
        df = pd.read_csv(filepath, sep=";")
        if df.columns.tolist() != colunas_esperadas:
            salva_status(nova_execucao, titulo=f"Erro ao Tratar Base da Receita: Arquivo {file} não possui as colunas esperadas",status="Erro")            
            return False
        
        #verificar se há cnpjs repetidos
        if len(df["cnpj"].tolist()) != len(df["cnpj"].unique().tolist()):
            salva_status(nova_execucao, titulo=f"Erro ao Tratar Base da Receita: Arquivo {file} possui cnpjs repetidos",status="Erro")            

            return False
        
        df_repetidos = df[df["cnpj"].isin(cnpjs_encontrados)]
        if len(df_repetidos.index) > 1:
            salva_status(nova_execucao, titulo=f"Erro ao Tratar Base da Receita: Arquivo {file} possui cnpjs repetidos com outro arquivo",status="Erro")            

            return False
        
        cnpjs_encontrados += df["cnpj"].unique().tolist()

        # colunas_telefone = ["TEL1", "TEL2", "TEL3"]
        # df_telefones = df[colunas_telefone]
        # for index, row in df_telefones.iterrows():
        #     tels = [row["TEL1"], row["TEL2"], row["TEL3"]]

    return True

if __name__ == "__main__":
    fase_1_gerador()


 


