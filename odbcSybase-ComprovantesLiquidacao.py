import json
import requests
import pyodbc

ip = '192.168.104.44'
port = 'Informe  a porta'
database = 'Informe o Database'
server = 'Informe o server'
username = 'Informe o login'
password = 'Informe a senha'


url = "https://contabil-sl.cloud.betha.com.br/contabil/service-layer/v2/api/comprovantes-liquidacoes"
token = "Informe o token"
headers = {
    'Content-Type': 'application/json',
    'Authorization': "Bearer " + token
}


strConn = f'Driver=Adaptive Server Anywhere 9.0;ENG={server};UID={username};PWD={password};DBN={database};LINKS=TCPIP(HOST={ip}:{port});'
conexao = pyodbc.connect(strConn)
cursor = conexao.cursor()
cursor.execute(" select chavDsk1Comprovante = dctos_fiscais.ano_exerc,"
"       chavDsk2Comprovante = dctos_fiscais.i_entidades,"
"       chavDsk3Comprovante = dctos_fiscais.i_tipo_dcto_fiscal,"
"       chavDsk4Comprovante = dctos_fiscais.i_numero_dcto_fiscal,"
"       chavDsk5Comprovante = if dctos_fiscais.tipo_juridico = 'J' then dctos_fiscais.cgc else dctos_fiscais.cpf endif,"
"       chavDsk6Comprovante = dctos_fiscais.num_docto,"
"       chavDsk7Comprovante = dctos_fiscais.tipo_docto,"
"       idComprovante = isNull(bethadba.dbf_get_id_gerado(1,'comprovantes',chavDsk1Comprovante,chavDsk2Comprovante,chavDsk3Comprovante,chavDsk4Comprovante,chavDsk5Comprovante,chavDsk6Comprovante,chavDsk7Comprovante), ''),"
"       idLiquidacao = isNull(bethadba.dbf_get_id_gerado(1, 'liquidacoes', liq.i_entidades, liq.ano_exerc, liq.i_empenhos, liq.i_liquidacao, liq.ano_exerc, 'LIQ') , ''),"
"       valor = dctos_fiscais.valor_dcto_fiscal,"
"       data = dctos_fiscais.data_emissao,"
"       empenho = liq.i_empenhos "
" from sapo.dctos_fiscais       "
" join sapo.liquidacao liq on (dctos_fiscais.ano_exerc = liq.ano_exerc and"
"                             dctos_fiscais.i_entidades = liq.i_entidades and"
"                             dctos_fiscais.num_docto = liq.i_liquidacao)                  "
" where dctos_fiscais.ano_exerc = 2020 and"
" dctos_fiscais.i_entidades = 8")
row = cursor.fetchall()

sucess = 0
error = 0
for x in row:
    objectJson = {
        "idIntegracao": "migracao",
        "content": {
            "liquidacao": {
                "id": int(x.idLiquidacao)
            },
            "comprovante": {
                "id": int(x.idComprovante)
            },
            "exercicio": x.chavDsk1Comprovante,
            "valor":  float(x.valor)
        }
    }
    jsonSend = json.dumps(objectJson)
    response = requests.request("POST", url=url, headers=headers, data=jsonSend)
    status = response.status_code
    if (status == 200):
        sucess += 1
        print("Comprovante da liquidacao do empenho:", str(x.empenho), "enviada com sucesso! ID Lote:", str(response.json()['idLote']))
    else:
        error += 1
        print("Erro ao enviar comprovante da liquidação do empenho: ", str(x.empenho), " status retorno:", str(status))

print("Total de Registros com sucesso:", str(sucess))
print("Total de Registros com erro:", str(error))
