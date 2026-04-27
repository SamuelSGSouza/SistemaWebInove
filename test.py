import pandas as pd
import re

def clean_phone_number(phone, apenas_celular:bool=False, apenas_fixos:bool=False) -> str:
    """Função para limpar e validar números de telefone"""
    if pd.isna(phone) or str(phone).strip() == '':
        return ''
    # Remove todos os caracteres não numéricos
    cleaned = re.sub(r'\D', '', str(phone))
    if len(cleaned) > 12 and str(cleaned[:2]) == "55":
        cleaned = cleaned[2:]
    ddd = cleaned[:2]

    valid_ddds = ["11","12","13","14","15","16","17","18","19","21","22","24","27","28","31","32","33","34","35","37","38","41","42","43","44","45","46","47","48","49","51","53","54","55","61","62","63","64","65","66","67","68","69","71","73","74","75","77","79","81","82","83","84","85","86","87","88","89","91","92","93","94","95","96"]
    if ddd not in valid_ddds:
        return ""

    #retirando ddds que apresentam problemas no discados URA:
    # if ddd in ["16", "34", "37"]:
    #     return ""

    telefone = cleaned[2:]

    repetidos = [f"{i}"*6 for i in range(0,10)] #Se tiver 6 dígitos repetidos, remove
    if any([rep in telefone for rep in repetidos]):
        return ""

    if len(telefone) == 9: #É celular
        if apenas_fixos:
            return ""

        if str(telefone[0]) == "9": #celular com 9 na frente
            return cleaned
    
        else: #tem 9 dígitos mas não é celular, portanto é um telefone inválido
            return ""
        
    elif len(telefone) == 8:
        if str(telefone[0]) in ["6", "7", "8","9"]: #É celular, mas sem 9 na frente
            return ddd + "9" + telefone
        
        else: #é um telefone fixo válido
            if apenas_celular:
                return ""
            else:
                if telefone[0] == "1":
                    return ""
                return cleaned


    else: #Tamanho inválido para telefone
        return ""
print(clean_phone_number("68992313876"))