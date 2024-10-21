from dotenv import load_dotenv
from src.infra.sql.sqlcon import sqlcon
from src.infra.log.customLog import customLog
from threading import current_thread
import os
import json
import requests
import time
import concurrent.futures as futures
import uuid

def memphis_finance_migration():
    load_dotenv()
    branch = 19 # 5 - CD FORTALEZA CE
    banc_cod = "" #"341" #input("Informe o Código do banco: ") #"341"
    we_number = "" #"4138165" #input("Informe o Nosso Número: ")#"4138165"
    max_threads = 10
    inicio = time.time()
    log = customLog(f"Migracao_Receber_{branch}")
    log.setLog(messege=f"Inicio da migração do financeiro.", print_terminal=True)
    connection = sqlcon()
    query = get_query(branch=branch, part_id="", banc_cod=banc_cod, we_number=we_number, theads=max_threads)
    df = connection.execute_query(query=query)
    with futures.ThreadPoolExecutor() as executor:
        custom_futures = []
        for index in range(max_threads):
            df_thread = df[df.THREAD == index]
            if len(df_thread) > 0:
                custom_futures.append(executor.submit(send_protheus, df=df_thread, log=log))
        for future in futures.as_completed(custom_futures):
            log.setLog(messege=future.result(), print_terminal=True)
    log.setLog(messege=f"Processo Finalizado com sucesso! tempo: {(time.time() - inicio)/60}", print_terminal=True)

def get_query(branch, part_id="", banc_cod="", we_number="", theads=1):
    query = f"""
    SELECT VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_NUMERODOCUMENTOCONTA % {theads} [THREAD]
        ,FILIAL.EP_FILIAL_CODIGO_PROTHEUS [E1_FILIAL]
        ,LEFT(CLIENTE.GE_CLIENTE_CODIGO_PROTHEUS, 6) [E1_CLIENTE]
        ,RIGHT(CLIENTE.GE_CLIENTE_CODIGO_PROTHEUS, 2) [E1_LOJA]
        ,FORMAT(VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_NUMERODOCUMENTOCONTA, '000000000') [E1_NUM]
        ,FORMAT(PARCELA.FI_PARCELA_NUMEROPARCELA, '00') [E1_PARCELA]
        ,'NF' [E1_TIPO]
        ,IIF(VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_CODIGO_DOCUMENTO = 12,'0101989','0101999') [E1_NATUREZ]
        ,'MEM' [E1_PREFIXO]
        ,CONVERT(VARCHAR(8), VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_DATA_CRIACAO, 112) [E1_EMISSAO]
        ,CONVERT(VARCHAR(8), VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_DATA_VENCIMENTO, 112) [E1_VENCTO]
        ,CONVERT(VARCHAR(8), VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_DATA_VENCIMENTO, 112) [E1_VENCREA]
        ,VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_VALOR [E1_VALOR]
        ,FORMAT(CONVERT(BIGINT, BOLETO.BC_BOLETO_NOSSONUMERO), '0000000000') [E1_IDCNAB]
        ,VENDEDOR.GE_VENDEDOR_CODIGO_PROTHEUS [E1_VEND1]
        ,CONVERT(DECIMAL(18,2), (SELECT 
            IIF(SUM(sub_BI.ValorTotal_NF) > 0 AND SUM(sub_BI.Comissao_Pedido) > 0,
                SUM(sub_BI.ValorTotal_NF * (sub_BI.Comissao_Pedido / 100)) /
                (SELECT COUNT(SUB_PARCELA.PK_FI_PARCELA_ID)
                FROM TB_FI_PARCELA SUB_PARCELA (NOLOCK)
                WHERE SUB_PARCELA.PK_FI_PARCELA_FI_CONTA_ID = 
                VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_CONTA_ID),
                0
            )
        FROM BI_Faturamento sub_BI (NOLOCK)
        INNER JOIN TB_FI_CONTAMOVIMENTO sub_CONTAMOVIMENTO (NOLOCK)
            ON sub_BI.ID_Movimento_NF = sub_CONTAMOVIMENTO.PK_FI_CONTAMOVIMENTO_CVS_MOVIMENTO_ID 
        WHERE sub_CONTAMOVIMENTO.PK_FI_CONTAMOVIMENTO_FI_CONTA_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_CONTA_ID
        GROUP BY sub_CONTAMOVIMENTO.PK_FI_CONTAMOVIMENTO_FI_CONTA_ID
        )) [E1_VALCOM1]
        ,(SELECT TIPOSJUROSDESCONTOS.PK_FI_TIPOSJUROSDESCONTOS_ID [ID_Memphis]
            ,TIPOSJUROSDESCONTOS.FI_TIPOSJUROSDESCONTOS_EhJuros [EhJuros]
            ,SUM(PARCELA_JUROSDESCONTOS.FI_PARCELA_JUROSDESCONTOS_VALOR) [FKD_VALOR]
        FROM TB_FI_PARCELA_JUROSDESCONTOS PARCELA_JUROSDESCONTOS (NOLOCK)
        INNER JOIN TB_FI_TIPOSJUROSDESCONTOS TIPOSJUROSDESCONTOS (NOLOCK)
            ON PARCELA_JUROSDESCONTOS.FK_FI_PARCELA_JUROSDESCONTOS_FI_TIPOSJUROSDESCONTOS_ID = TIPOSJUROSDESCONTOS.PK_FI_TIPOSJUROSDESCONTOS_ID
        WHERE PARCELA_JUROSDESCONTOS.FK_FI_PARCELA_JUROSDESCONTOS_FI_PARCELA_FI_CONTA_ID = PARCELA.PK_FI_PARCELA_FI_CONTA_ID
            AND PARCELA_JUROSDESCONTOS.FK_FI_PARCELA_JUROSDESCONTOS_FI_PARCELA_ID = PARCELA.PK_FI_PARCELA_ID
        GROUP BY TIPOSJUROSDESCONTOS.PK_FI_TIPOSJUROSDESCONTOS_ID
            ,TIPOSJUROSDESCONTOS.FI_TIPOSJUROSDESCONTOS_EhJuros
        FOR JSON PATH
        ) [FKD]
        ,VENDEDOR.PK_GE_VENDEDOR_EP_FUNCIONARIO_ID [VENDEDOR_ID]
        ,VENDEDOR.GE_VENDEDOR_NOME [VENDEDOR_NOME]
        ,FORNECEDOR_PESSOA.GE_PESSOA_CPF [VENDEDOR_CNPJ]
    FROM VW_FI_MOVIMENTO (NOLOCK) 
    INNER JOIN TB_EP_FILIAL FILIAL (NOLOCK) 
        ON FILIAL.PK_EP_FILIAL_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_FILIAL_ID  
    INNER JOIN TB_GE_CLIENTE CLIENTE (NOLOCK)
        ON CLIENTE.PK_GE_CLIENTE_GE_PESSOA_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_CODIGO_PESSOA
    INNER JOIN TB_GE_VENDEDORCLIENTE VENDEDORCLIENTE (NOLOCK)
        ON CLIENTE.PK_GE_CLIENTE_GE_PESSOA_ID = VENDEDORCLIENTE.FK_GE_VENDEDORCLIENTE_GE_CLIENTE_ID
        AND GETDATE() BETWEEN VENDEDORCLIENTE.GE_VENDEDORCLIENTE_DATAINICIAL AND VENDEDORCLIENTE.GE_VENDEDORCLIENTE_DATAFINAL
    INNER JOIN TB_GE_VENDEDOR VENDEDOR (NOLOCK)
        ON VENDEDORCLIENTE.FK_GE_VENDEDORCLIENTE_GE_VENDEDOR_ID = VENDEDOR.PK_GE_VENDEDOR_EP_FUNCIONARIO_ID
    LEFT JOIN TB_GE_FORNECEDOR FORNECEDOR (NOLOCK)
        ON VENDEDOR.FK_GE_VENDEDOR_GE_FORNECEDOR_ID = FORNECEDOR.PK_GE_FORNECEDOR_GE_PESSOA_ID
    LEFT JOIN TB_GE_PESSOA FORNECEDOR_PESSOA (NOLOCK)
        ON FORNECEDOR.PK_GE_FORNECEDOR_GE_PESSOA_ID = FORNECEDOR_PESSOA.PK_GE_PESSOA_ID
    INNER JOIN TB_FI_PARCELA PARCELA (NOLOCK)
        ON VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_PARCELA_ID = PARCELA.PK_FI_PARCELA_ID
    LEFT JOIN TB_BC_BOLETOPARCELA BOLETOPARCELA (NOLOCK)
        ON BOLETOPARCELA.FK_BC_BOLETOPARCELA_FI_PARCELA_ID = VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_PARCELA_ID 
    LEFT JOIN TB_BC_BOLETO BOLETO (NOLOCK)
        ON BOLETO.PK_BC_BOLETO_ID = BOLETOPARCELA.FK_BC_BOLETOPARCELA_BC_BOLETO_ID
    WHERE 1=1
        AND (VW_FI_MOVIMENTO_EHPAGAR = 0)
        AND VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_STATUS <> 'PG'
    """
    if branch:
        query += f" AND VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_FILIAL_ID = {branch} "
    if banc_cod:
        query += f" AND BOLETO.BC_BOLETO_BANCOCODIGO = {banc_cod} "
    if we_number:
        query += f" AND BOLETO.BC_BOLETO_NOSSONUMERO = {we_number} "
    if part_id:
        query += f" AND VW_FI_MOVIMENTO.VW_FI_MOVIMENTO_PARCELA_ID = {part_id} "
    return query

def send_protheus(df, log):
    idThead = current_thread().getName()
    url = f'{os.getenv('URL_ENDPOINT')}/integracoes/financeiro/receber'
    for index, row in df.iterrows():
        request_id = str(uuid.uuid4())
        request = {
            "data": {
                "id": request_id,
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
                    "E1_IDCNAB": row['E1_IDCNAB'],
                    "E1_VEND1": row['E1_VEND1'],
                    "E1_VALCOM1": row['E1_VALCOM1'],
                },
                "VENDEDOR": {
                    "VENDEDOR_ID": row['VENDEDOR_ID'],
                    "VENDEDOR_NOME": row['VENDEDOR_NOME'],
                    "VENDEDOR_CNPJ": str(row['VENDEDOR_CNPJ']).replace(',','.'),
                }
            }
        } 
        if row["FKD"] != None:
            request["data"]["FKD"] = json.loads(row["FKD"])
        log.setLog(f"Request id: {request_id}, Enviando Titulo Numero:{row["E1_NUM"]}, Parcela:{row["E1_PARCELA"]}, Valor: {row["E1_VALOR"]}, Thead: {idThead}")
        response = requests.post(url=url, data=json.dumps(request))
        response_body = json.loads(response.content)
        if response.ok:
            log.setLog(f"Request id: {request_id}, Financiero inserido!")
        else:
            log.setError(f"Request id: {request_id}, Erro na requsição: ")
            log.setError(response_body["message"])
    return f"Thread {idThead} Finalizou!"

if __name__ == "__main__":
    memphis_finance_migration()