
import os, sys, psycopg2
from time import localtime, strftime



##Cria uma tupla vazia para receber os parâmetros de conexão
par_nome = []
par = []

##Obtém o diretório do programa
diretorio =os.path.dirname(os.path.abspath(sys.argv[0]))

#Faz a leitura do arquivo config.conf e armazena os dados nas tuplas

try:
    with open(diretorio+'\config.conf','r') as arq:
        for linha in arq:
            conteudo = linha
            a,conteudo = conteudo.split(' = ')
            conteudo = conteudo.replace('\n','') 
            par.append(conteudo)
            par_nome.append(a)


except Exception as e:
    #Obtem o horário de execução para poder salvar no nome do arquivo
    time =strftime("%Y-%m-%d-%H", localtime())
    
    #Cria um arquivo, caso não exista, para gravar as informações de erro
    with open(diretorio+'\error-'+time+'.log','a') as f:
        
        #Grava no arquivo o horário e a mensagem que ocorreu
        f.write(strftime("%Y-%m-%d %H:%M:%S", localtime()) +': '+ str(e))
        
        #Fecha o arquivo
        f.close()
        
        #Encerra o programa
        sys.exit('Ocorreu um erro, verifique o arquivo de log para mais informações.')
            
if len(par_nome) != 5:
    
     #Obtem o horário de execução para poder salvar no nome do arquivo
    time =strftime("%Y-%m-%d-%H", localtime())
    
    #Cria um arquivo, caso não exista, para gravar as informações de erro
    with open(diretorio+'\error-'+time+'.log','a') as f:
        
        #Grava no arquivo o horário e a mensagem que ocorreu
        f.write((strftime("%Y-%m-%d %H:%M:%S", localtime()) +': '+ "Número de parâmetros inválidos. Recebidos:"+str(par_nome)) + " Esperados: ['host', 'database', 'user', 'password', 'arquivo']\n")  
        
        #fecha o arquivo
        f.close()
        #Encerra o programa
        sys.exit('Ocorreu um erro, verifique o arquivo de log para mais informações.')
    



#Valida se os dados de conexão estão corretos
try:
    con = psycopg2.connect(host=par[0],database = par[1], \
    user = par[2],password = par[3])
    cur  = con.cursor()


    
#Valida se há erro na conexão com o banco
except psycopg2.OperationalError as e:

    time =strftime("%Y-%m-%d-%H", localtime())
    
    #Cria um arquivo, caso não exista para gravar as informações de erro
    with open(diretorio+'\error-'+time+'.log','a') as f:
        e = str(e).replace('\n','')
        #Grava no arquivo o horário e a mensagem que ocorreu
        f.write(strftime("%Y-%m-%d %H:%M:%S", localtime()) +': '+ (e)+'\n')
        
        #Fecha o arquivo
        f.close()   
    
        #Fecha o programa
        sys.exit('Ocorreu um erro, verifique o arquivo de log para mais informações.')
        

   


#Função para criar tabela
def create_table():
    comando=("CREATE TABLE IF NOT EXISTS estabelecimento (\
	ID int not NULL PRIMARY KEY,\
	LONG int ,\
	LAT int,\
	SETCENS bigint,\
	AREAP bigint,\
	CODDIST smallint,\
	DISTRITO varchar(20),\
	CODSUBPREF smallint,\
	SUBPREF varchar(30),\
	REGIAO5 varchar(10),\
	REGIAO8 varchar(10),\
	ESTABELECI varchar(255),\
	ENDERECO varchar(100),\
	BAIRRO varchar(100),\
	TELEFONE varchar(13),\
	CEP varchar(10) ,\
	CNES varchar(20),\
	SA_DEPADM smallint,\
	DEPADM varchar(9),\
	SA_TIPO smallint,\
	TIPO varchar(255),\
	SA_CLASSE smallint,\
	CLASSE varchar(255),\
	LEITOS smallint)")
        
    #Executa o comando create que está dentro
    #da variável comando
    cur.execute(comando)
    
    #Grava a transação
    con.commit()

#Cria a tabela se ela não existir
create_table()

#Tenta gravar conteúdo da planilha no arquivo.
try:
    #atribuii o arquivo csv para a variável arq
    arq =open (diretorio+'/'+par[4],'r')
    
    #desconsidera a linha do cabeçalho
    next(arq) 

    #insere o conteúdo do arquivo na tabela estabelecimento
    cur.copy_from(arq,'estabelecimento',sep=';')
    
    #commita a inserção dos dados
    con.commit()

    #fecha o arquivo
    arq.close()  

    #fecha conexão
    con.close()
    
    
#Se houver alguma exceção na inserção dos dados grava no arquivo de log
except Exception as e:
    
    #Obtem o horário de execução para poder salvar no nome do arquivo
    time =strftime("%Y-%m-%d-%H", localtime())
    
    #Cria um arquivo, caso não exista para gravar as informações de erro
    with open(diretorio+'\error-'+time+'.log','a') as f:
        e = str(e).replace('\n','')
        #Grava no arquivo o horário e a mensagem que ocorreu
        f.write(strftime("%Y-%m-%d %H:%M:%S", localtime()) +': '+ (e)+'\n')
        
        #Fecha o arquivo
        f.close()   
        
        #Fecha conexão
        con.close()
        
        #Fecha o programa
        sys.exit('Ocorreu um erro, verifique o arquivo de log para mais informações.')
        

##Tive que alterar o delimitador do texto para ponto e vírgula, pois há virgula no endereço
##https://onlinecsvtools.com/change-csv-delimiter
##Fontes
## Obter o horário do logo: https://stackoverflow.com/questions/415511/how-to-get-the-current-time-in-python
## Gravar o conteúdo do arquivo no banco https://www.psycopg.org/docs/cursor.html
## Erros e exceções https://www.psycopg.org/docs/errors.html

