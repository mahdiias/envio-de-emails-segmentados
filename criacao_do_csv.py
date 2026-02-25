import pandas as pd

# cria estrutura padrão do histórico de alertas
df_historico = pd.DataFrame(columns=[
    "registro_id",
    "tipo_alerta",
    "grupo",
    "data_envio"
])

# salva arquivo CSV inicial vazio
df_historico.to_csv("historico_alertas.csv", index=False)

print("Arquivo de histórico criado com sucesso.")
