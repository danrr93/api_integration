#projeto de integracao, mobi e mw mysql bd via api de integracao mw
#arquivo eventsnumber.txt serve para armazenar o numero total de eventos enviados
#arquivo lastevent.txt armazena o ultimo id do evento enviado
#para saber se eventos estao atualizados e nao reenviar eventos antigos,
#se pega o numero total de eventos disponiveis e compara, caso numero maior
#que o armazenado no eventsnumber.txt, comeca a procurar por eventos com o id
#maior que o armazenado em lastevent.txt para enviar
import requests, json, time

#script configurations
#MW client
#HONDA PLANTA 0
mw_client_id = "4a151167-29dc-45f5-9194-bd2c5a3a4fe5"
mw_client_email_api = 'mobihonda_mwapi@mwautomacao.com'
mw_client_password_api = 'mobi@!2023'
mwtoken = ""
mwtokenfile = "./mw_token_file.txt"

#Mobi client
mobi_client_name = "MOB"
mobi_client_login = "89bunzl9170"
mobi_client_user = "RFIDUser"
mobi_client_password = "RFIDUser12"
mobitokenfile = "./mobi_api_token.txt"
mobitoken = ""
planta = 1

#variavel que armazena matricula antiga necessaria para editar colaborador
oldregistration = ""

#lista de colaboradores para serem adicionados/editados
add_packetEmployees = []
upd_packetEmployees = []

#counters de colaboradores
employeesedited_counter = 0
employeesok_counter = 0

#todos os colaboradores retornados das API MW E MOB
employees_packetfromapi_mw = []
employees_packetfromapi_mob = []

#demitidos numero
dem_counter = 0

#verifica arquivos importantes
try:
    # Try to open the file for reading
    with open(mobitokenfile, 'r') as file:
        # Read the contents of the file
        mobitoken = file.read()
except FileNotFoundError:
    # If the file doesn't exist, create it
    print("File mobi token doesn't exist. Creating it...")
    with open(mobitokenfile, 'w') as file:
        file.write("")

try:
    # Try to open the file for reading
    with open(mwtokenfile, 'r') as file:
        # Read the contents of the file
        mwtoken = file.read()
except FileNotFoundError:
    # If the file doesn't exist, create it
    print("File mobi token doesn't exist. Creating it...")
    with open(mwtokenfile, 'w') as file:
        file.write("")


def mobiloginApi():
    #Here we update tokens
    #to obtain all args, run getClients first
    print('starting login integration')
    print(mobi_client_user, mobi_client_password, mobi_client_login)

    url = 'https://mobintegration.azurewebsites.net/api/Login'
    body = '{"UserID": "%s", "AccessKey": "%s", "IdCliente": "%s"}' % (mobi_client_user, mobi_client_password, mobi_client_login)

    while True:
        res = requests.post(url, json=body)

        count = 0
        if res.status_code != 200:
            count += 1
            print("Request failed by status code:", str(res.status_code))

            time.sleep(1)

            if count > 4:
                return res.status_code

            continue
        else:
            print("200 OK")
            break

    jsondata = json.loads(res.content)

    if jsondata["authenticated"] == False:
        print("Error at authenticate client:", mobi_client_login)
        return "ERROR"

    with open(mobitokenfile, 'w') as f:
        f.write(jsondata["accessToken"])

    global mobitoken
    mobitoken = jsondata["accessToken"]
    return 200

def getAllMobiEmployees(codplanta):
    print("Obtendo colaboradores da mob")
    #url definition and read from apitoken file
    url = 'https://mobintegration.azurewebsites.net/api/default/obterfuncionarioplanta?codigoPlanta={codplanta}'.format(codplanta=str(codplanta))

    #authorization header definition to access api
    headers = {
        "Authorization": "Bearer {apitoken}".format(apitoken=mobitoken)
    }

    #http request
    n = 0
    while True:
        res = requests.get(url, headers = headers)

        if res.status_code == 401:
            print("Login pendente em API MOB")
            mobiloginApi()
            return False

        if res.status_code != 200:
            n += 1
            #attempts to http req, if failed return status code
            if n == 2:
                print("All atempts for http request failed:", str(res.status_code))
                return False
            
            print("Request failed by status code:", str(res.status_code))
            time.sleep(3)
            continue
        else:
            print("200 OK")
            break

    global employees_packetfromapi_mob
    employees_packetfromapi_mob = json.loads(res.content)

    print("Preenchimento de pacote com todos os colaboradores da Mobi efeutado via API")
    print("Total:", len(employees_packetfromapi_mob))

    return True

def mwloginApi():
    print('starting mw api login')
    headers = {
        'Content-Type': 'application/json'
    }
    url = 'https://tecnologias.mwautomacao.com.br/api/login'
    body = {"email": mw_client_email_api, "password": mw_client_password_api}
    res = requests.post(url, headers=headers, json=body)

    if res.status_code != 200:
        print("mw token request failed with status code:", str(res.status_code))
        return res.status_code
    
    jsonres = json.loads(res.content)
    mwtoken = jsonres["access_token"]
    with open(mwtokenfile, "w") as f:
        f.write(mwtoken)

    return 200

def getAllMWEmployees():
    print("Pegando colaboradores da API MW")
    currentpage = 1
    #url definition and read from apitoken file
    #authorization header definition to access api
    headers = {
        "Authorization": "Bearer {apitoken}".format(apitoken=mwtoken)
    }

    #http request
    n = 0
    while True:
        url = 'https://tecnologias.mwautomacao.com.br/api/employee?page={cpage}'.format(cpage=currentpage)
        res = requests.get(url, headers = headers)

        if res.status_code == 401:
            print("Login pendente em API MW")
            mwloginApi()
            return False

        if res.status_code != 200:
            n += 1
            #attempts to http req, if failed return status code
            if n == 2:
                print("All atempts for http request failed:", str(res.status_code))
                return False
            
            print("Request failed by status code:", str(res.status_code))
            time.sleep(3)
            continue
        else:
            global employees_packetfromapi_mw
            try:
                jsondata = json.loads(res.content)
            except Exception:
                print('failed at json, try to get a new token')
                mwloginApi()
                return False

            if len(jsondata["data"]) == 0:
                print("sem colaboradores nesta pagina da api MW")
                break
            
            for employee in jsondata["data"]:
                employees_packetfromapi_mw.append(employee)
            
            currentpage += 1

    print("Preenchimento de pacote com todos os colaboradores da MW efeutado via API")
    print("Total:", len(employees_packetfromapi_mw))

    return True





def findMwEmployee(badge, register):
    #authorization header definition to access api
    print("procurando colaborador na api mw com o registro:", register, "e cracha:", badge)
    headers = {
        "Authorization": "Bearer {apitoken}".format(apitoken=mwtoken)
    }

    url = 'https://tecnologias.mwautomacao.com.br/api/employee?registration={register}'.format(register=register)

    res = requests.get(url, headers = headers)

    if res.status_code == 401:
        print("falha de autenticaca na api mw")
        return "", 401

    if res.status_code != 200:
        print("falha na obtencao do colaborador, status code:", str(res.status_code))
        return "", res.status_code
    
    jsondata = json.loads(res.content)
    if len(jsondata["data"]) == 0:
        print("registro colaborador ainda nao encontrado na api mw, verificando cracha...")
        url = 'https://tecnologias.mwautomacao.com.br/api/employee?badge={badge}'.format(badge=badge)

        try:
            res = requests.get(url, headers=headers)
        except TimeoutError:
            print("TimeoutError!")
            return "", "TIMEOUT"
        
        if res.status_code != 200:
            print("Falha ao pesquisar cracha na API MW, status code:", str(res.status_code))
            return "", res.status_code
        
        jsonbadge = json.loads(res.content)

        if len(jsonbadge["data"]) > 0:
            print("encontrado cracha:", jsonbadge["data"][0]["badge"], "na api mw")
            return jsonbadge["data"], 200
        #cadastra colaborador via api
    else:
        print("registro do colaborador encontrado")
    
    return jsondata["data"], 200


def saveEmployeeInPacket(jsonemployee, type):
    if type == "ADD":

        if jsonemployee["departamento"] is None:
            jsonemployee["departamento"] = "SEM SETOR"
        else:
            if len(jsonemployee["departamento"]) == 0:
                jsonemployee["departamento"] = "SEM SETOR"   

        if jsonemployee["cargo"] is None:
            jsonemployee["cargo"] = "SEM CARGO"
        else:
            if len(jsonemployee["cargo"]) == 0:
                jsonemployee["cargo"] = "SEM CARGO"

        body = {"name": jsonemployee["nome"], 
                "badge": jsonemployee["codigoCracha"], 
                "registration": jsonemployee["matricula"],
                "birthday": jsonemployee["dt_Nascimento"],
                "gender": "MASCULINO",
                "admission_date": jsonemployee["dt_Admissao"],
                "demission_date": jsonemployee["dt_Demissao"],
                "user_type": "USUARIO",
                "department": jsonemployee["departamento"],
                "occupation": jsonemployee["cargo"],
                "business_unit": jsonemployee["planta"],
                "shift": "DIURNO",
                "training": "",
                "aso": "",
                "device_type":["dosadora"]}
    
    if type == "ADD":
        global add_packetEmployees
        add_packetEmployees.append(body)
        print("Colaborador nome:", jsonemployee["nome"], "e matricula:", jsonemployee["matricula"], "adicionado ao pacote ADD")

    if type == "UPD":
        global upd_packetEmployees
        upd_packetEmployees.append(jsonemployee)
        print("Colaborador nome:", jsonemployee["name"], "e matricula:", jsonemployee["registration"], "adicionado ao pacote UPD")
    

   
def saveEmployeesInApiMw():
    print("Enviando pacote de colaboradores para API MW...")
    #need to receive the mobi json employee data
    url = 'https://tecnologias.mwautomacao.com.br/api/employee'

    headers = {
        'Content-Type': 'application/json',
        "Authorization": "Bearer {apitoken}".format(apitoken=mwtoken)
    }

    jsondata = json.dumps(add_packetEmployees)

    res = requests.post(url, headers=headers, data=jsondata)

    if res.status_code != 200:
        print("status code:", str(res.status_code))

    return True

def checkEmployeeData(mobijsonemployee, mwjsonemployee):
    print("verificando dados")
    isedited = False
    #print(mwjsonemployee)
    #print("--------------------------------------------------------")
    #print(mobijsonemployee)
    if(mobijsonemployee["codigoCracha"] == mwjsonemployee["badge"] and
       mobijsonemployee["nome"] == mwjsonemployee["name"] and
       mobijsonemployee["matricula"] != mwjsonemployee["registration"]):

        mwjsonemployee['oldregistration'] = mwjsonemployee["registration"]
        mwjsonemployee["registration"] = mobijsonemployee["matricula"]
        print("PRONTO PARA EDITAR MATRICULA")
        isedited = True

    #verifica o restante dos dados se há divergencia
    if (mobijsonemployee["afastado"] == True and
        mobijsonemployee["codigoCracha"] is not None and
        mwjsonemployee["badge"] is not None):

        if not "X" in mwjsonemployee["badge"]:
            print("colaborador afastado!")
            try:
                mwjsonemployee["badge"] = "X" + mobijsonemployee["codigoCracha"]
            except TypeError:
                print("codigoCracha do colaborador mobi inválido")
            else:
                isedited = True
        #inativar colaborador
        #altere o cracha do colaborador para nao ser mais reconhecido

    if mobijsonemployee["dt_Demissao"] is not None:
        if len(mobijsonemployee["dt_Demissao"]) > 0:
            if mobijsonemployee["dt_Demissao"] != mwjsonemployee["demission_date"]:
                print("colaborador demitido")
                mwjsonemployee["demission_date"] = mobijsonemployee["dt_Demissao"]
                isedited = True
                global dem_counter
                dem_counter += 1
                #inativar colaborador
                #apenas inserir data de demissao!

    if mobijsonemployee["nome"] is not None:
        if mwjsonemployee["name"] != mobijsonemployee["nome"].upper():
            if len(mobijsonemployee["nome"]) != 0:
                print("atualizando nome do colaborador")
                mwjsonemployee["name"] = mobijsonemployee["nome"]
                isedited = True

    if mobijsonemployee["codigoCracha"] is not None:
        if mwjsonemployee["badge"] != mobijsonemployee["codigoCracha"].upper():
            if len(mobijsonemployee["codigoCracha"]) != 0:
                    print("atualizando cracha do colaborador")
                    if mobijsonemployee["afastado"] == False:
                        mwjsonemployee["badge"] = mobijsonemployee["codigoCracha"]
                        isedited = True

    if mobijsonemployee["departamento"] is not None:
        if mwjsonemployee["department"] != mobijsonemployee["departamento"].upper():
            if len(mobijsonemployee["departamento"]) != 0:
                print("atualizando departamento do colaborador")
                mwjsonemployee["department"] = mobijsonemployee["departamento"]
                isedited = True

    if mobijsonemployee["cargo"] is not None:
        if mwjsonemployee["occupation"] != mobijsonemployee["cargo"].upper():
            if len(mobijsonemployee["cargo"]) != 0:
                    print("atualizando cargo do colaborador")
                    mwjsonemployee["occupation"] = mobijsonemployee["cargo"]
                    isedited = True

    if mobijsonemployee["planta"] is not None:
        if mwjsonemployee["business_unit"] != mobijsonemployee["planta"].upper():
            if len(mobijsonemployee["planta"]) != 0:
                    print("atualizando unidade empresarial(planta) do colaborador")
                    mwjsonemployee["business_unit"] = mobijsonemployee["planta"]
                    isedited = True

    return mwjsonemployee, isedited    

def editEmployeeInMwApi():
    print('editando colaboradores em mw api')

    headers = {
        'Content-Type': 'application/json',
        "Authorization": "Bearer {apitoken}".format(apitoken=mwtoken)
    }
    url = ""

    for colaborador in upd_packetEmployees:
        if "oldregistration" in colaborador:
            #isto significa que a matricula foi trocada
            url = 'https://tecnologias.mwautomacao.com.br/api/employee/{registration}'.format(registration=colaborador["oldregistration"])
        else:
            url = 'https://tecnologias.mwautomacao.com.br/api/employee/{registration}'.format(registration=colaborador["registration"])


        print("editando colaborador:", colaborador["name"], "na mw api")



        body = {"name": colaborador["name"], 
                "badge": colaborador["badge"], 
                "registration": colaborador["registration"],
                "birthday": colaborador["birthday"],
                "gender": "MASCULINO",
                "admission_date": colaborador["admission_date"],
                "demission_date": colaborador["demission_date"],
                "user_type": "USUARIO",
                "department": colaborador["department"],
                "occupation": colaborador["occupation"],
                "business_unit": colaborador["business_unit"],
                "shift": "DIURNO",
                "training": "",
                "aso": "",
                "device_type":["dosadora"]}
    
    
        bodyjson = json.dumps(body)
        #print(bodyjson)
        #print(url)
        errorcounter = 10
        while True:
            res = requests.put(url, headers=headers, data=bodyjson)

            if res.status_code != 200:
                print("falha ao editar, status code:", str(res.status_code))
                errorcounter -= 1
                if errorcounter == 0:
                    break
                
            else:
                print("colaborador:",colaborador["name"], "editado com sucesso!")
                print(res.content)
                break
            

def getSendEvents():
    #varifica data e hora do ultimo evento enviado
    with open('./lastevent.txt', 'r') as f:
        lastidevent = f.read()

    if len(lastidevent) == 0:
        lastidevent = 0

    lastidevent = int(lastidevent)

    #pega os eventos da MW API e envia para MOBI API
    mwurl = 'https://tecnologias.mwautomacao.com.br/api/events/dosadora'
    mwheaders = {
        "Authorization": "Bearer {apitoken}".format(apitoken=mwtoken)
    }
    print(mwurl)
    urlmobi = 'https://mobintegration.azurewebsites.net/api/VendingMachine/receberConsumoEpi'
    mobiheaders = {
        "Authorization": "Bearer {apitoken}".format(apitoken=mobitoken)
    }   

    print("tentando obter eventos da MW API")
    res = requests.get(mwurl, headers=mwheaders)
    if res.status_code != 200:
        print("falha ao obter eventos, status code:", str(res.status_code))
        return False
    print("formatando eventos para json, isto pode demorar um pouco...")
    jsonmwevents = json.loads(res.content)

    #bate quantidade de eventos com quantidade ja enviada
    totalevents = len(jsonmwevents["data"])

    print("lendo numero de eventos em eventsnumber.txt")
    with open('./eventsnumber.txt', 'r') as f:
        totaleventssended = f.read()
    totaleventssended = int(totaleventssended)

    if totalevents == totaleventssended:
        print("nenhum novo evento encontrado")
        return False

    for mwevent in jsonmwevents["data"]:
        #print(mwevent)
        if mwevent["id"] <= lastidevent:
            continue

        #pega o produto, codref e codcontrole
        product = mwevent["product_name"]
        codcontrole = product.split('-')[0]
        codref = product.split('-')[1]

        mobievent = {"Matricula": mwevent["registration"],
                     "EpisConsumidos": [{
                         "CodigoControle": codcontrole,
                         "Referencia": codref,
                         "QuantidadeConsumida": mwevent['quantity_apply'],
                         "Excecao": True,
                         "DataHora": mwevent["datetime_use"]
                     }]}

        print(mobievent)

        print("tentando enviar evento para API da MOBI")
        res = requests.post(urlmobi, headers=mobiheaders, json=mobievent)
        if res.status_code != 200:
            print("falha ao enviar evento, status code:", str(res.status_code))
            continue

        jsonres = json.loads(res.content)
        if jsonres["ok"] == False:
            print("Erro, evento nao enviado!")
            print(jsonres["mensagem"])
            continue

        print("evento enviado")
        print(res.content)
        with open('./lastevent.txt', 'w') as f:
            f.write(str(mwevent["id"]))

        totaleventssended += 1
        with open('./eventsnumber.txt', 'w') as f:
            f.write(str(totaleventssended))
    

def mainloop():
    errorcounter = 10
    while True:
        if len(employees_packetfromapi_mob) == 0:
            if not getAllMobiEmployees(planta):
                errorcounter -= 1
                print("falha em adquirir colaboradores da Mob")
                continue

        if len(employees_packetfromapi_mw) == 0:
            if not getAllMWEmployees():
                errorcounter -= 1
                print("falha em adquirir colaboradores da MW")
                continue

        if errorcounter == 0:
            print("Falha nas funcoes, finalizando sistema")
            break

        if len(employees_packetfromapi_mob) > 0:
            print("Comecando comparacao entre colaboradores...")
            print("")

            for mob_colab in employees_packetfromapi_mob:
                print("")
                addneeded = True
                print(mob_colab["nome"], mob_colab["matricula"])
                print("verificando se este mob colaborador ja existe na mw api")

                for mw_colab in employees_packetfromapi_mw:
                    if (mw_colab["name"] == mob_colab["nome"] and
                        mw_colab["registration"] == mob_colab["matricula"]):
                        addneeded = False
                        print("Este colaborador ja existe na mw api, verificando se foi editado")
                        packetedit, bedited = checkEmployeeData(mob_colab, mw_colab)

                        if(bedited):
                            print("inserindo colaborador no pacote para editar")
                            saveEmployeeInPacket(packetedit, "UPD")
                        else:
                            print("colaborador nao editado, tudo ok")
                            global employeesok_counter
                            employeesok_counter += 1

                        break

                if addneeded:
                    print("Colaborador nao existe na api mw, adicionando ao pacote")
                    saveEmployeeInPacket(mob_colab, "ADD")

                print("")


            print("Encerrando sincronismo de colaboradores")
            print("Numero de colaboradores para adicionar:", len(add_packetEmployees))
            print("Numero de colaboradores que precisam serem editados:", len(upd_packetEmployees))
            print("Numero de colaboradores já adicionados e sem necessidade de editar na MW:", employeesok_counter)
            print("demitidos:", dem_counter)
            print("")

            print('Comecando requisicoes para sincronizar...')

            print("inserindo colaboradores na api mw")
            while True:
                if not saveEmployeesInApiMw():
                    errorcounter -= 1
                    if errorcounter == 0:
                        print("Falha ao enviar colaboradores")
                        return
                    else:
                        continue

                else:
                    print("colaboradores inseridos com sucesso!")
                    break

            print("editando colaboradores pendentes...")

            editEmployeeInMwApi()
            break

    getSendEvents()

                    

mainloop()

