
import re

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import  StringType, DecimalType

from pyspark.sql.functions import trim, regexp_replace, col, udf

import pyspark.sql.functions as F




class claseSpark():   
    def __init__(self):
        
        self.CreaSparkContext()
        
    def CreaSparkContext(self):                  
        self.conf = SparkConf().setAppName("Aperturas").setMaster("local[*]")
        self.sc = SparkContext().getOrCreate(conf = self.conf);
    
        self.sqlContext = SQLContext(self.sc)
        self.sqlContext.sql("set spark.sql.shuffle.partitions=1")
        print('\n>>>   Contexto de Spark creado\n')        
        
    def pararSpark(self):
        self.sc.stop()

def create_spark_session():
    builder = SparkSession.builder
    builder = builder.enableHiveSupport().master("local[*]")

    spark = builder.getOrCreate()
    
    # spark.conf.set("spark.executor.memory", '8g')
    # spark.conf.set('spark.executor.cores', '3')
    # spark.conf.set('spark.cores.max', '12')
    # spark.conf.set("spark.driver.memory",'8g')
    
    sqlContext = SQLContext( spark )
    return spark, sqlContext
        
def CreaDFSpark(sqlContext,server_path,ListaNombres,Fecha,LocalOServer):
    
    dfs=[]
    if LocalOServer == "local":
        for Nombre in ListaNombres:
                    
            pathCompletoFichero=server_path+"/"+Nombre
            print(pathCompletoFichero)
            print("Creando el dataframe de Spark ",Nombre,end="\r")                   
            dfs.append( sqlContext.read.format('com.databricks.spark.csv')\
                                       .options(header='true',delimiter=';', inferschema='true')\
                                       .load(pathCompletoFichero) )
            print(" \r Creado el dataframe de Spark ",Nombre,"\n",end="")   
    
    return dfs



def quitaEspacios_y_poneMayusculasNombColumna(dfss):
    # Dado un conjunto de dataframes de spark, se quitan los espacios
    # en blanco del principio y del final de cada nombre de columna
    # y se ponen en mayusculas
    dfssSal = []
    for dfs in dfss:
        ListaNombreCol = dfs.schema.names
        for NombreCol in ListaNombreCol:
            NombreColMod = NombreCol.rstrip().lstrip().upper()
            dfs = dfs.withColumnRenamed (NombreCol, NombreColMod)
        
        dfssSal.append(dfs)
    
    return dfssSal


def quitaPuntosNombresColumnas(dfss):
    # Como muchas veces da problemas los puntos en los nombres de las
    # columnas, se sustituyen estos puntos por un espacio
    
    dfsSalida = []

    for df in dfss:
        ListaNombresColumnas = df.schema.names
        
        for NombreColumna in ListaNombresColumnas:
           
            NombreColumnaMod = re.sub("\\."," ",NombreColumna)
            df = df.withColumnRenamed ( NombreColumna, NombreColumnaMod )
        
        dfsSalida.append(df)
    
    
    return dfsSalida


def quitaEspaciosPrincipioFinDFs(dfss):
    # Quita espacios al principio y al final de los datos de un
    # dataframe de spark
    
    dfsSalida = []
    
    for df in dfss:
        ListaNombresColumnas = df.schema.names

        for NombreColumna in ListaNombresColumnas:
            
            df = df.withColumn(NombreColumna, trim(df[NombreColumna]))
    
        dfsSalida.append(df)
        
    return dfsSalida


def CambiaColumnasPuntos_Y_Comas(DF,ListaColumnas):
    #Se cambia, en las columnas elegidas, el punto por nada y la coma por punto
    for Columna in ListaColumnas:
        
        DF_Nuevo = DF      .withColumn(Columna, regexp_replace(col(Columna), ","  , ","))
        DF = DF_Nuevo
    # print("      Conseguido cambiar las comas por puntos\n")
    return DF_Nuevo


def BorrarColumnasNullSpark(df):
    """
    This function drops all columns which contain null values.
    :param df: A PySpark DataFrame
    """
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v > 0]
    df = df.drop(*to_drop)
    return df

def cogeUltimas_n_Cifras_y_quitaCerosInicio(dfs_Input, nombreCol, n):
    # Se cambia los valores de la columna nombreCol de forma que solo se 
    # obtengan los n ultimos valores y que si los primeros son 0. Se borran
    def get_n_UltimasCifras(string1):
        
        
            
        string1 = str( string1 )
        string2 = string1[-n:]
        
        if string2.isnumeric():
            pattern = r"^0*" #expresion regular para quitar los primeros 0
            
            return re.sub(pattern,"",string2)
        else: return "No numerico"

        
    udfValueToCategory = udf(get_n_UltimasCifras, StringType())
    
    return dfs_Input.withColumn( nombreCol, udfValueToCategory( nombreCol ) )  

def SumaValoresColumna(DF,ColumnaNumero):
    
    #Sumamos todos los valores de una columna de un DataFrame pasandolo a RDD
    #para ganar rapidez
    SumaRDD = DF.rdd.map(lambda x: (ColumnaNumero,x[ColumnaNumero])).reduceByKey(lambda x,y: x + y)
             
    return SumaRDD


def ObtenerNumeroColumna(DF,columna):
    # Se obtiene la posicion( en numero) de la columna "columna"
    listaColumnas = DF.columns    
    num=0
        
    for columnafor in listaColumnas:
        if columnafor==columna:
            break
        num = num+1
    return num


def cambiaColumnas_A_Decimal(DF,ListaColumnas): 
    # Se cambia el tipo de dato de las columnas elegidas a decimal
    for Columna in ListaColumnas:       
        DF_Nuevo = DF.withColumn(Columna, DF[Columna].cast(DecimalType(15,3)))   
    
    # print("      Conseguido cambiar a double\n")
    return DF_Nuevo

def anadeconcat( dfs_INPUT, nombreNuevaCol, concat1, concat2):
    # Se crea una nueva columna de un df_INPUT con el nombre
    # nombreNuevaCol y los valores concat1,concat2 concatenados  
    def concatenar(string1,string2):    
        return str(string1) + "-" + str(string2)
    
    udfValueToCategory = udf(concatenar, StringType())
    
    return dfs_INPUT.withColumn(nombreNuevaCol, udfValueToCategory(concat1,concat2))    


def copiaColumnaSparkString(df_INPUT, ColumnaCopiar, NombreColumnaNueva):
    
    def copiarColumn(valor): return valor
        
    udfcopiarColumn = udf(copiarColumn, StringType())
    
    return df_INPUT.withColumn( NombreColumnaNueva, udfcopiarColumn( ColumnaCopiar ) )  

def CambiaSignoColumnaSapark(sqlContext,DFInput,NombreColumna):
    # Se cambia el signo de una columna "NombreColumna"
    # de un df de spark DFInput . Debe de ser con formato numero int,float,double   
    
    DFOutput = DFInput.withColumn( NombreColumna, - DFInput[NombreColumna] )
    
    return DFOutput


def ConvierteDoubleEnFormatoStringRaro ( MyNum, NumDecimales ):
    # Esta funcion convierte un numero con formato double o float
    # en un string con el siguiente esqueleto:
    #
    #    MyNum           MyNumModif
    # 1246514.12312 -> 1.245.514,12
    
    MyNum    = float(MyNum)
    MyNumStr = str  (MyNum)
    
    # Comprobamos si el numero de entrada es negativo o positivo
    # para quitar el simbolo de '+' '-' si lo hubiera y anadirselo
    # al final del proceso
    esNegativo = False
    
    if MyNumStr[0] == '-':
        esNegativo = True
        MyNumStr = MyNumStr[1:]
        
    if MyNumStr[0] == '+' :
        esNegativo = False
        MyNumStr = MyNumStr[1:]

    MyNumStrEntero = str(MyNumStr)
    MyNumStrDecim  = ''
    
    #Vemos la posicion del punto 
    DotPos = MyNumStr.find('.')  
    
    if not DotPos == -1:
        
        # Reemplazamos el punto por la coma 
        MyNumStr = MyNumStr.replace(".",",")
        
        # Se obtienen un String con solo los decimales necesarios
        # y otro String con el valor entero del numero (sin decimales)
        MyNumStrDecim  = MyNumStr[ DotPos : DotPos + NumDecimales + 1]
        MyNumStrEntero = MyNumStr[:DotPos]
    
    # **************** SE IMPLEMENTAN LOS PUNTOS ***************************
    contador = 1   
    puntosAnadidos = 0
    lastPos = len(MyNumStrEntero)
    MyNumModif = ''
    
    # Se implementan los puntos de 3 en 3. 
    for caracter in reversed(MyNumStrEntero):
        
        if contador == 3:
            
           StrEntrePuntos = MyNumStrEntero[ lastPos - contador : lastPos ] 
           #print( "\n" , StrEntrePuntos )
           MyNumModif = '.' + StrEntrePuntos + MyNumModif 
           
           lastPos = lastPos - ( puntosAnadidos + 1 )*contador
           ++puntosAnadidos
           contador = 0
                                
        contador = contador + 1
        #print(contador)
        
    # Se anade los primeros valores 
    MyNumModif = MyNumStrEntero[ : lastPos-puntosAnadidos * 3 ] + MyNumModif   
    print(MyNumModif,"\n")
    # Se anade los valores decimales
    MyNumModif = MyNumModif + MyNumStrDecim
    
    if MyNumModif[0] == '.':
        MyNumModif = MyNumModif[1:]
    
    if esNegativo == True:
        MyNumModif ='-'+ MyNumModif
              
    return MyNumModif


def FormatoAColumnas( DFInput, listaColumnas): 
# Dado un DF de Spark y unos nombres de su columna incluidas en la lista
# listaColumnas, se aplica mediante udf una transformacion de los valores
# de dichas columnas al formato de numero requerido. Esto es, 
# ConvierteDoubleEnFormatoStringRaro
     
    for NombreColumna in listaColumnas:
        colsInt = udf(lambda z: ConvierteDoubleEnFormatoStringRaro ( z, 2 ), StringType())    
        DFInput = DFInput.withColumn( NombreColumna,colsInt(NombreColumna))

    return DFInput
