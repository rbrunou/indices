import yfinance as yf
import pandas as pd
from datetime import date, datetime, timedelta

data_inicial = datetime.strptime('2020-03-05', '%Y-%m-%d')

dti = pd.bdate_range(data_inicial, date.today(), freq="D")

#gera um dataframe com todos os meses desde a data inicial para compilar os dados mensais
mti = pd.bdate_range(data_inicial, date.today(), freq="m")
mti = mti.strftime('%Y-%m-%d').tolist()
mti.append(date.today().strftime('%Y-%m-%d'))

cotacoes_importadas_yahoo = pd.DataFrame(index = dti)

###################################################################################################
#Cria um dataframe com o nome indices, para comparar os índices:
#IBOV, SP500, IPCA, IGPM e CDI
#Primeiro apenas copia os dados do IBOV e do SP500 importados do yahoo 
indices = pd.DataFrame(index = dti)
ativos_para_baixar = 'BRL=X,' + '^BVSP,' + '^GSPC'
cotacoes_importadas_yahoo = yf.download(ativos_para_baixar, start=data_inicial)[['Adj Close']]
indices['Dolar']   =    cotacoes_importadas_yahoo.iloc[:, 0]
indices['IBOV']   =     cotacoes_importadas_yahoo.iloc[:, 1]
indices['SP500']   =    cotacoes_importadas_yahoo.iloc[:, 2]
indices.fillna(method='ffill', inplace=True)


#Para importar indicadores do banco central, foi criada a função consulta_bc
# https://www3.bcb.gov.br/sgspub/localizarseries/localizarSeries.do?method=prepararTelaLocalizarSeries
def consulta_bc(codigo_bcb):
  url = 'http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json'.format(codigo_bcb)
  df = pd.read_json(url)
  df['data'] = pd.to_datetime(df['data'], dayfirst=True)
  df.set_index('data', inplace=True)
  return df

#Importa os indicadores do banco central
print("")
print("Importando dados Banco Central")
indices['IPCA'] = consulta_bc(433)
indices['IGPM'] = consulta_bc(189)
indices['CDI'] = consulta_bc(12)

#Substitui todos os dados NAN para 0
indices.fillna(0, inplace=True) 
###################################################################################################

###################################################################################################
#Cria um dataframe com os índices importados, de tal forma que consigamos analisálos de forma
#acumulada desde o primeiro investimento feito.
indices_acumulados = pd.DataFrame(index = dti)

#Calcula a porcentagem acumulada do IBOV
#(indices[0]['IBOV']*indices[1]['IBOV']*indices[n]['IBOV']-1)
#pct_change -> retorna a diferença percentual entre o dado atual e o anterior
#cumprod -> realiza a multiplicação acumulada entre os dados da coluna especificada
indices_acumulados['IBOV'] = round(((1+(indices['IBOV'].pct_change())).cumprod()-1),2)
indices_acumulados.iloc[0]=0

indices_acumulados['SP500'] = round(((1+(indices['SP500'].pct_change())).cumprod()-1),2)
indices_acumulados.iloc[0]=0

indices_acumulados['Dolar'] = round(((1+(indices['Dolar'].pct_change())).cumprod()-1),2)
indices_acumulados.iloc[0]=0

#Nos indicadores abaixo, o retorno do banco central já é em porcentagem, enquanto o SP500 e o 
#IBOV é em pontos
indices_acumulados['IPCA'] = round(((1+(indices['IPCA']/100)).cumprod()-1),2)
indices_acumulados.iloc[0]=0

indices_acumulados['IGPM'] = round(((1+(indices['IGPM']/100)).cumprod()-1),2)
indices_acumulados.iloc[0]=0

indices_acumulados['CDI'] = round(((1+(indices['CDI']/100)).cumprod()-1),2)
indices_acumulados.iloc[0]=0

###################################################################################################
indices_mensais = pd.DataFrame(index = mti, columns=['IBOV', 'SP500', 'IPCA', 'IGPM', 'CDI', 'Dolar'])

for i in range(len(indices_mensais)):
    indices_mensais['IBOV'][mti[i]] = indices_acumulados.loc[indices_acumulados.index == mti[i]].IBOV[0]
    indices_mensais['SP500'][mti[i]]  = indices_acumulados.loc[indices_acumulados.index == mti[i]].SP500[0]
    indices_mensais['Dolar'][mti[i]]  = indices_acumulados.loc[indices_acumulados.index == mti[i]].Dolar[0]
    indices_mensais['IPCA'][mti[i]]  = indices_acumulados.loc[indices_acumulados.index == mti[i]].IPCA[0]
    indices_mensais['IGPM'][mti[i]]  = indices_acumulados.loc[indices_acumulados.index == mti[i]].IGPM[0]
    indices_mensais['CDI'][mti[i]]  = indices_acumulados.loc[indices_acumulados.index == mti[i]].CDI[0]

indices_mensais.to_csv('out.csv') 

