import pandas as pd
from alertas import gerar_alertas

# caminhos dos arquivos CSV utilizados no processamento
arquivos_csv = {
    "Formulario_A": "caminho/para/formulario_a.csv",
    "Formulario_B": "caminho/para/formulario_b.csv",
    "Formulario_C": "caminho/para/formulario_c.csv",
    "Registros_Extras": "caminho/para/registros_extras.csv"
}
dados = {}

# leitura dos arquivos e carregamento em dicionário
for nome, caminho in arquivos_csv.items():
    dados[nome] = pd.read_csv(caminho)

# execução da função principal de geração de alertas
gerar_alertas(dados, enviar_email_flag=True)

print("Processamento de alertas executado com sucesso.")
