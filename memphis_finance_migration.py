from dotenv import load_dotenv
from src.infra.sql.sqlcon import sqlcon
from src.infra.log.customLog import customLog
from threading import current_thread
import os
import json
import requests
import time
import concurrent.futures as futures

def memphis_finance_migration():
    load_dotenv()
    log = customLog("Migracao_Receber")
    log.setLog(messege=f"Inicio da migração do financeiro.", print_terminal=True)
    inicio = time.time()
    connection = sqlcon()
    branch = 5 # 5 - CD FORTALEZA CE
    max_threads = 10
    query = get_query(branch=branch, part_id="", theads=max_threads)
    df = connection.execute_query(query=query)
    # custom_threads = []
    # for index in range(max_threads):
    #     df_thread = df[df.THREAD == index]
    #     if len(df_thread) > 0:
    #         custom_thread = threading.Thread(target=send_protheus, args=(df_thread, index, log))
    #         custom_thread.start()
    #         custom_threads.append(custom_thread)
    # for thread in custom_threads:
    #      thread.join()
    with futures.ThreadPoolExecutor() as executor:
        custom_futures = []
        for index in range(max_threads):
            df_thread = df[df.THREAD == index]
            if len(df_thread) > 0:
                custom_futures.append(executor.submit(send_protheus, df=df_thread, idThead=index, log=log))
        for future in futures.as_completed(custom_futures):
            log.setLog(messege=future.result(), print_terminal=True)
    log.setLog(messege=f"Processo Finalizado com sucesso! tempo: {(time.time() - inicio)/60}", print_terminal=True)

def get_query(branch, part_id="", theads=1):
    query = f"""
    SELECT TOP 500 VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_NUMERODOCUMENTOCONTA % {theads} [THREAD]
    ,FILIAL.EP_FILIAL_CODIGO_PROTHEUS [E1_FILIAL]
	,LEFT(CLIENTE.GE_CLIENTE_CODIGO_PROTHEUS, 6) [E1_CLIENTE]
	,RIGHT(CLIENTE.GE_CLIENTE_CODIGO_PROTHEUS, 2) [E1_LOJA]
	,FORMAT(VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_NUMERODOCUMENTOCONTA, '000000000') [E1_NUM]
	,FORMAT(PARCELA.FI_PARCELA_NUMEROPARCELA, '00') [E1_PARCELA]
	,'NF' [E1_TIPO]
	,'0204970' [E1_NATUREZ]
	,'MEM' [E1_PREFIXO]
	,CONVERT(VARCHAR(8), VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_DATA_CRIACAO, 112) [E1_EMISSAO]
	,CONVERT(VARCHAR(8), VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_DATA_VENCIMENTO, 112) [E1_VENCTO]
	,CONVERT(VARCHAR(8), VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_DATA_VENCIMENTO, 112) [E1_VENCREA]
	,VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_VALOR [E1_VALOR]
    ,VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_NUMERO_DOCUMENTO_PARCELA [E1_IDCNAB]
	,(SELECT CASE TIPOSJUROSDESCONTOS.FI_TIPOSJUROSDESCONTOS_EhJuros
			 WHEN 1 THEN '000003' 
			 WHEN 0 THEN '000004'
			 END [FKD_CODIGO]
		,SUM(PARCELA_JUROSDESCONTOS.FI_PARCELA_JUROSDESCONTOS_VALOR) [FKD_VALOR]
	FROM TB_FI_PARCELA_JUROSDESCONTOS PARCELA_JUROSDESCONTOS
	INNER JOIN TB_FI_TIPOSJUROSDESCONTOS TIPOSJUROSDESCONTOS
		ON PARCELA_JUROSDESCONTOS.FK_FI_PARCELA_JUROSDESCONTOS_FI_TIPOSJUROSDESCONTOS_ID = TIPOSJUROSDESCONTOS.PK_FI_TIPOSJUROSDESCONTOS_ID
	WHERE PARCELA_JUROSDESCONTOS.FK_FI_PARCELA_JUROSDESCONTOS_FI_PARCELA_FI_CONTA_ID = PARCELA.PK_FI_PARCELA_FI_CONTA_ID
	AND PARCELA_JUROSDESCONTOS.FK_FI_PARCELA_JUROSDESCONTOS_FI_PARCELA_ID = PARCELA.PK_FI_PARCELA_ID
	GROUP BY TIPOSJUROSDESCONTOS.FI_TIPOSJUROSDESCONTOS_EhJuros
	FOR JSON PATH
	) [FKD]
    FROM VW_FI_MOVIMENTO (NOLOCK) 
    INNER JOIN TB_EP_FILIAL FILIAL (NOLOCK) 
        ON FILIAL.PK_EP_FILIAL_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_FILIAL_ID  
    INNER JOIN TB_GE_CLIENTE CLIENTE (NOLOCK)
        ON CLIENTE.PK_GE_CLIENTE_GE_PESSOA_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_CODIGO_PESSOA 
    INNER JOIN TB_FI_PARCELA PARCELA (NOLOCK)
        ON VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_PARCELA_ID = PARCELA.PK_FI_PARCELA_ID
    LEFT JOIN TB_BC_BOLETOPARCELA BOLETOPARCELA (NOLOCK)
        ON BOLETOPARCELA.FK_BC_BOLETOPARCELA_FI_PARCELA_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_PARCELA_ID 
    LEFT JOIN TB_BC_BOLETO BOLETO (NOLOCK)
        ON BOLETO.PK_BC_BOLETO_ID = BOLETOPARCELA.FK_BC_BOLETOPARCELA_BC_BOLETO_ID
    WHERE 1=1
    AND (VW_FI_MOVIMENTO_EHPAGAR = 0)
    AND VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_FILIAL_ID = {branch}
    AND (VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_STATUS = 'PC' OR VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_STATUS = 'PD')
    """
    if part_id:
        query += f" AND VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_PARCELA_ID = {part_id} "
    return query

def send_protheus(df, idThead, log):
    url = f'{os.getenv('URL_ENDPOINT')}/integracoes/financeiro/receber'
    for index, row in df.iterrows():
        request = {
            "data": {
                "empresa": "01",
                "filial": row['E1_FILIAL'],
                "origem": "Memphis",
                "process": "",
                "SE1": {
                    "E1_CLIENTE": row['E1_CLIENTE'],
                    "E1_EMISSAO": row['E1_EMISSAO'],
                    "E1_FILIAL": row['E1_FILIAL'],
                    "E1_LOJA": row['E1_LOJA'],
                    "E1_NATUREZ": row['E1_NATUREZ'],
                    "E1_NUM": row['E1_NUM'],
                    "E1_PARCELA": row['E1_PARCELA'],
                    "E1_PREFIXO": row['E1_PREFIXO'],
                    "E1_TIPO": row['E1_TIPO'],
                    "E1_VALOR": row['E1_VALOR'],
                    "E1_VENCREA": row['E1_VENCREA'],
                    "E1_VENCTO": row['E1_VENCTO'],
                    "E1_IDCNAB": row['E1_IDCNAB']
                }
            }
        } 
        if row["FKD"] != None:
            request["data"]["FKD"] = json.loads(row["FKD"])
        log.setLog(f"Enviando Titulo Numero:{row["E1_NUM"]}, Parcela:{row["E1_PARCELA"]}, Valor: {row["E1_VALOR"]}, Thead: {current_thread().getName()}")
        # response = requests.post(url=url, data=json.dumps(request))
        # response_body = json.loads(response.content)
        # if response.ok:
        #     log.setLog("Financiero inserido!")
        # else:
        #     log.setError("Erro na requsição: ")
        #     log.setError(response_body["message"])
    return f"Thread {current_thread().name} Finalizou!"

if __name__ == "__main__":
    memphis_finance_migration()