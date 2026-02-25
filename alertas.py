# bibliotecas utilizadas
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.message import EmailMessage
import os

# configurações principais de envio e histórico
EMAIL_REMETENTE = ""
EMAIL_SENHA = ""
SMTP_SERVIDOR = "smtp.gmail.com"
SMTP_PORTA = 587
CAMINHO_HISTORICO = "historico_alertas.csv"
TZ = ZoneInfo("America/Sao_Paulo")

# mapeamento de grupo/unidade -> lista de e-mails responsáveis
DESTINATARIOS_POR_GRUPO = {
    # "GRUPO_A": ["email1@email.com", "email2@email.com"],
}

# função responsável por enviar e-mail para uma lista de destinatários
def enviar_email_lista(assunto, mensagem, lista_destinatarios):
    msg = EmailMessage()
    msg["From"] = EMAIL_REMETENTE
    msg["To"] = EMAIL_REMETENTE  # remetente visível
    msg["Bcc"] = ", ".join(lista_destinatarios)  # envio oculto para a lista
    msg["Subject"] = assunto
    msg.set_content(mensagem)

    # conexão com servidor SMTP e envio do e-mail
    with smtplib.SMTP(SMTP_SERVIDOR, SMTP_PORTA) as server:
        server.starttls()  # inicia conexão segura
        server.login(EMAIL_REMETENTE, EMAIL_SENHA)
        server.send_message(msg)

# função principal que gera os alertas
def gerar_alertas(dados, enviar_email_flag=True):
  
    # carregamento das tabelas recebidas
    df_formulario_a = dados["Formulario_A"]
    df_formulario_b = dados["Formulario_B"]
    df_formulario_c = dados["Formulario_C"]
    df_registros_extra = dados["Registros_Extras"]

    # leitura do histórico de alertas já enviados
    if os.path.exists(CAMINHO_HISTORICO):
        df_historico = pd.read_csv(CAMINHO_HISTORICO)
    else:
        df_historico = pd.DataFrame(columns=[
            "registro_id",
            "tipo_alerta",
            "grupo",
            "data_envio"
        ])

    # contagem de registros auxiliares agrupados por ID
    registros_por_id = (
        df_registros_extra.groupby("registro_id")
        .size()
        .reset_index(name="qtd_registros")
    )

    # construção da base consolidada a partir dos merges
    df_base = (
        df_formulario_b[["registro_id", "campo_obrigatorio", "timestamp", "grupo_acesso"]]
        .merge(
            df_formulario_c[["registro_id", "campo_resultado", "observacao"]],
            on="registro_id",
            how="left"
        )
        .merge(registros_por_id, on="registro_id", how="left")
        .merge(
            df_formulario_a[["registro_id", "responsavel"]],
            on="registro_id",
            how="left"
        )
    )

    # substitui valores nulos da contagem por zero
    df_base["qtd_registros"] = df_base["qtd_registros"].fillna(0)

    alertas = []  # lista final de alertas gerados
    alertas_por_grupo = {}  # organização dos alertas por grupo

    # percorre cada registro consolidado
    for _, row in df_base.iterrows():
        registro_id = row["registro_id"]
        grupo = row["grupo_acesso"]

        # ignora grupos que não possuem e-mails cadastrados
        if grupo not in DESTINATARIOS_POR_GRUPO:
            continue

        # valida se o campo obrigatório está vazio ou inválido
        campo_vazio = str(row["campo_obrigatorio"]).strip() not in ["1", "True", "true"]

        if campo_vazio:
            agora = datetime.now(TZ)

            # cria lista do grupo se ainda não existir
            if grupo not in alertas_por_grupo:
                alertas_por_grupo[grupo] = []

            # adiciona descrição resumida do alerta
            alertas_por_grupo[grupo].append(
                f"Registro ID: {registro_id} | Responsável: {row['responsavel']}"
            )

            # registra o alerta no histórico
            df_historico = pd.concat([
                df_historico,
                pd.DataFrame([{
                    "registro_id": registro_id,
                    "tipo_alerta": "CAMPO_OBRIGATORIO_AUSENTE",
                    "grupo": grupo,
                    "data_envio": agora
                }])
            ], ignore_index=True)

            # adiciona alerta na lista de retorno
            alertas.append({
                "registro_id": registro_id,
                "tipo_alerta": "CAMPO_OBRIGATORIO_AUSENTE",
                "gravidade": "CRITICO",
                "responsavel": row["responsavel"],
                "grupo": grupo,
                "email_enviado": enviar_email_flag,
                "data_alerta": agora
            })

    # envio consolidado de e-mails por grupo
    if enviar_email_flag:
        for grupo, lista_alertas in alertas_por_grupo.items():

            destinatarios = DESTINATARIOS_POR_GRUPO[grupo]

            corpo = f"""
🚨 ALERTAS CRÍTICOS — GRUPO {grupo}

Total de registros com inconsistência: {len(lista_alertas)}

----------------------------------------
"""
            corpo += "\n".join(lista_alertas)
            corpo += f"\n\nData do envio: {datetime.now(TZ).strftime('%d/%m/%Y %H:%M')}"

            assunto = f"🚨 ALERTA AUTOMÁTICO — Grupo {grupo}"

            enviar_email_lista(assunto, corpo, destinatarios)

    # salva o histórico atualizado em CSV
    df_historico.to_csv(CAMINHO_HISTORICO, index=False)

    # retorna DataFrame com todos os alertas gerados
    return pd.DataFrame(alertas)
