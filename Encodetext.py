####################################################################
#Archivo: Encodetext.py
#Autor:Equipo1
#Fecha de actualización: 04/02/20201
#Descripción:Standarizar formato y limpieza de columnas en una tabla 
####################################################################

import pandas as pd
import cx_Oracle
import re
from unicodedata import normalize
import string
cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_9")
connection = cx_Oracle.connect("SCH_UNIVERSAL", "un1v4rs4l20",
                               "10.5.112.35:1521/dgpp")
# try:
cursor = connection.cursor()
query = """select id, DESCB,fecha_hoy,fecha_man,des,fecha_com,fecha_ac from test_mail"""
df = pd.read_sql(query, con=connection)
df_ora = df

def  Encodetext( bPunt,bDate, bFillna,bAccent,var_cols,df_ora ):
    print(df_ora)
    # uso de replace para espacios en todas columnas
    df_ora.replace(regex=[r'\n|\t|\r'], value=[' '],inplace=True)
    df_ora.replace(regex=[r'\s+'], value=[' '],inplace=True)
    trim_str = lambda x: x.strip() if isinstance(x, str) else x
    df_ora.applymap(trim_str)

    def standar_date(x):
      '''
      Esta función convierte un formato yyyy-mm-dd hh:mm:ss
      or yyyy-mm-dd y retorna en formato yy/mm/dd hh:mm:ss
      x es la columna tipo de dato datetime.
      '''  
      if bDate == True :  
        if not str(x) or re.findall('[A-Za-z]+', str(x)):
            return x
        digits= re.findall(r'\d+', str(x))
        a = digits[0][2:4]
        b = digits[1]
        c = digits[2]
        hms = ''
        if len(digits) == 6:
            hor = digits[3]
            minu = digits[4]
            seg = digits[5]
            hms = f' {hor}:{minu}:{seg}'
        return f'{a.zfill(2)}/{b.zfill(2)}/{c.zfill(2)}{hms}'
      else: 
        return x 

    #funcion standar accents con unicodedata
    def standar_accents(x):
       '''
       Esta función convierte columnas tipo varchar2 con acentos
       áéíóúÁÉÍÓÚ en aeiouAEIOU
       x es la columna tipo de dato object.
       '''  
       if  bAccent == True:
        x = re.sub(r'([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+',
                   r'\1',normalize( "NFD", str(x)), 0, re.I)
        return normalize( 'NFC', str(x))    
       else: 
        return x

    #declaramos listas por tipos de objetos
    listcolobj = []
    listcoldate = []
    listcolfloat = []
    #listamos columnas por tipo de objeto 
    for i,j in list(zip(df_ora.columns,df_ora.dtypes)):
        if j=='object' :
         if i in var_cols:
            continue
         listcolobj.append(i)
        if j == 'datetime64[ns]':
         if i in var_cols:
            continue    
         listcoldate.append(i)
        if j == 'float64' :
         if i in var_cols:
            continue     
         listcolfloat.append(i)

    #realizamos limpieza por tipos de columnas objetos
    df_obj = df_ora.loc[:, listcolobj] 
    df_obj.replace(regex=['[%s]' % re.escape(string.punctuation)],value=[' '],inplace=bPunt)
    df_obj = df_obj.applymap(standar_accents) 
    df_ora[listcolobj] = df_obj

    #realizamos limpieza por tipos de columnas date
    df_date = df_ora.loc[:,listcoldate]
    df_date = df_date.applymap(standar_date) 
    df_ora[listcoldate] = df_date

    #realizamos limpieza por tipos de columnas float
    df_float = df_ora.loc[:,listcolfloat]
    df_float.fillna(0, inplace=bFillna)
    df_ora[listcolfloat] = df_float
    print("tabla limpia \n")
    print(df_ora)

# poner columnas que no seran afectadas por su tipo o sino ['NINGUNO']
var_cols = ['FECHA_HOY','FECHA_MAN','FECHA_COM']
# boolear True o false para cambiar la puntuación,acentos,fecha,fillna
Encodetext( True,False,True,False,var_cols,df_ora)