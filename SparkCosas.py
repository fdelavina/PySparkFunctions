import re, sys
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
         
("set spark.sql.shuffle.partitions=10")
import pyspark.sql.functions as F

from pyspark.sql.functions import regexp_replace, col, lit, udf, trim, \
                                  monotonically_increasing_id, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,FloatType,DecimalType


from pyspark.sql import Window

from librerias.TRAD_functions_server import get_files

def FuncionCreaDFPandas(fichero,sheet,nombre):
    
    if sheet=="":
        print("   Cargando ",nombre) 
        DFpanda = pd.read_excel(fichero,
                                keep_default_na=""
                                )
        
        print(" \r  Cargado ",nombre, "\n",end="") 
        
    else:
        
        print("   Cargando hoja ",sheet,end="\r" ) 
        
        DFpanda = pd.read_excel(fichero,
                               sheet_name = sheet,
                               dtype = str,
                               keep_default_na=""
                               )
        
        print("  \r Cargada hoja ",sheet, "\n",end="")
    
    return DFpanda


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
    

def CambiaColumnas_A_Double(DF,ListaColumnas): #funciona
    # Se cambia el tipo de dato de las columnas elegidas a float
    for Columna in ListaColumnas:       
        DF_Nuevo = DF.withColumn(Columna, DF[Columna].cast(DecimalType(15,3)))   
    
    # print("      Conseguido cambiar a double\n")
    return DF_Nuevo

def CambiaColumnasPuntos_Y_Comas(DF,ListaColumnas):
    #Se cambia, en las columnas elegidas, el punto por nada y la coma por punto
    for Columna in ListaColumnas:
        
        DF_Nuevo = DF      .withColumn(Columna, regexp_replace(col(Columna), "\\.", ""   ))\
                           .withColumn(Columna, regexp_replace(col(Columna), ","  , "\\."))
        DF = DF_Nuevo
    # print("      Conseguido cambiar las comas por puntos\n")
    return DF_Nuevo

def SumaValoresColumna(DF,ColumnaNumero):
    
    #Sumamos todos los valores de una columna de un DataFrame pasandolo a RDD
    #para ganar rapidez
    Suma = DF.rdd.map(lambda x: (ColumnaNumero,x[ColumnaNumero])).reduceByKey(lambda x,y: x + y).collect()[0][1]
    
    # if isinstance(Suma, numbers.Number):
    #     print("      Conseguido hacer la suma y es igual a \n",Suma)
    # else:
    #     print("      No ha conseguido hacer la suma\n")    
  
    return Suma


def ObtenerNumeroColumna(DF,columna):
    # Se obtiene la posicion( en numero) de la columna "columna"
    listaColumnas = DF.columns    
    num=0
    
    for columnafor in listaColumnas:
        if columnafor==columna:
            break
        num = num+1
            
    return num

def CalculaSiNoCuadraDiferenciasInforSLD(DF_INFOR):
    #Vemos si la suma de los valores de la columna diferencia filtrada por TT
    #es igual que la suma de los valores de la columna diferencia filtrada por lo demas
    
    DF_INFOR1 = CambiaColumnasPuntos_Y_Comas(DF_INFOR,["DIFERENCIA"])
    DF_INFOR2 = CambiaColumnas_A_Double(DF_INFOR1,["DIFERENCIA"])
      
    DF1 = DF_INFOR2
    DF2 = DF_INFOR2
               
    DF_filtradoPorTT = DF1.filter((col("SIT CED") == "TT") )  
    
    NumeroColumnaDIFERENCIA = ObtenerNumeroColumna(DF_INFOR,"DIFERENCIA")
    
    SumaTT = SumaValoresColumna(DF_filtradoPorTT,NumeroColumnaDIFERENCIA)
    
    #print(SumaTT)
    
    DF_filtradoPorCA_CN_NR = DF2.filter((col("SIT CED") == "CA") | \
                                        (col("SIT CED") == "CN") | \
                                        (col("SIT CED") == "NR")   ) 
  
        
    SumaCA_CN_NR = SumaValoresColumna(DF_filtradoPorCA_CN_NR,NumeroColumnaDIFERENCIA) 
    #print(SumaCA_CN_NR)
    TOL=1    
    if SumaTT+TOL >= SumaCA_CN_NR and SumaTT-TOL <= SumaCA_CN_NR:
        Si_o_No = True
       # print(">>> La suma en INFORSLD en DIFERENCIAS filtrada por TT y CA,CN,NR es igual por tanto se puede continuar sin problema")
    else:
        Si_o_No = False
        
    return Si_o_No



def CalculaSiNoCuadraDiferenciasLisconten(dfs_LISTCONTEN,diferencianombre):
    #Vemos si la suma de los valores de la columna diferencia filtrada por 0
    #es igual que la suma de los valores de la columna diferencia filtrada por 33
    
    dfs_LISTCONTEN1 = CambiaColumnasPuntos_Y_Comas(dfs_LISTCONTEN,[diferencianombre])
    dfs_LISTCONTEN2 = CambiaColumnas_A_Double(dfs_LISTCONTEN1,[diferencianombre])
       
    DF1 = dfs_LISTCONTEN2
    DF2 = dfs_LISTCONTEN2    
            
    DF_filtradoPor0 = DF1.filter((col("`TIP.REG.`") == "0") )  
    
    NumeroColumnaDIFERENCIA = ObtenerNumeroColumna(dfs_LISTCONTEN,diferencianombre)
    
    SumaTT = SumaValoresColumna(DF_filtradoPor0,NumeroColumnaDIFERENCIA)
       
    DF_filtradoPo33 = DF2.filter((col("`TIP.REG.`") == "33")  ) 
  
        
    SumaCA_CN_NR = SumaValoresColumna(DF_filtradoPo33,NumeroColumnaDIFERENCIA) 
    
    TOL=1
    if SumaTT+TOL >= SumaCA_CN_NR and SumaTT-TOL <= SumaCA_CN_NR:
        Si_o_No = True
       # print(">>> La suma en LISTCONTEN en",diferencianombre, " filtrada por 0 y 33 es igual por tanto se puede continuar sin problema")
    else:
        Si_o_No = False
        
    return Si_o_No

def CalculaSiNoCuadraDiferenciasCuadImp(dfs_CUADIMP):
    #Vemos si la suma de los valores de la columna diferencia filtrada por TT
    #es igual que la suma de los valores de la columna diferencia filtrada por lo demas
    
    dfs_CUADIMP1 = CambiaColumnasPuntos_Y_Comas(dfs_CUADIMP,["DIF TOT IMP"])
    dfs_CUADIMP2 = CambiaColumnas_A_Double(dfs_CUADIMP1,["DIF TOT IMP"])
      
    DF1 = dfs_CUADIMP2
    DF2 = dfs_CUADIMP2
               
    DF_filtradoPorTT = DF1.filter((col("EVENTO") == 'TOT. COMPRA') )  
    
    NumeroColumnaDIFERENCIA=ObtenerNumeroColumna(dfs_CUADIMP,"DIF TOT IMP")
    
    SumaTT = SumaValoresColumna(DF_filtradoPorTT,NumeroColumnaDIFERENCIA)
    
    #print(SumaTT)
    
    DF_filtradoPorCA_CN_NR = DF2.filter((col("EVENTO") == "DESC IMPAGO")  ) 
  
        
    SumaCA_CN_NR = SumaValoresColumna(DF_filtradoPorCA_CN_NR,NumeroColumnaDIFERENCIA) 
    #print(SumaCA_CN_NR)
    TOL=1    
    if SumaTT+TOL >= SumaCA_CN_NR and SumaTT-TOL <= SumaCA_CN_NR:
        Si_o_No = True
       # print(">>> La suma en INFORSLD en DIFERENCIAS filtrada por TT y CA,CN,NR es igual por tanto se puede continuar sin problema")
    else:
        Si_o_No = False
        
    return Si_o_No

def CalculaSiNoCuadraDiferenciasCuadImpUci(dfs_CUADIMPUCI):
    #Vemos si la suma de los valores de la columna diferencia filtrada por TT
    #es igual que la suma de los valores de la columna diferencia filtrada por lo demas
    
    dfs_CUADIMPUCI = dfs_CUADIMPUCI.withColumnRenamed('DIF.IMPAGADOS' ,'DIF IMPAGADOS')
    
    dfs_CUADIMPUCI1 = CambiaColumnasPuntos_Y_Comas(dfs_CUADIMPUCI,['DIF IMPAGADOS'])
    dfs_CUADIMPUCI2 = CambiaColumnas_A_Double(dfs_CUADIMPUCI1,['DIF IMPAGADOS'])
      
    DF1 = dfs_CUADIMPUCI2
    DF2 = dfs_CUADIMPUCI2
               
    DF_filtradoPorTT = DF1.filter((col("EVENTO") == 'TOT. FONDO') )  
    
    NumeroColumnaDIFERENCIA = ObtenerNumeroColumna(dfs_CUADIMPUCI2,'DIF IMPAGADOS')
    
    SumaTT = SumaValoresColumna(DF_filtradoPorTT,NumeroColumnaDIFERENCIA)
       
    DF_filtradoPorCA_CN_NR = DF2.filter( (col("EVENTO") == "DESC IMPAG") | \
                                         (col("EVENTO") == 'DESC. REAL' ) )   
        
    SumaCA_CN_NR = SumaValoresColumna(DF_filtradoPorCA_CN_NR,NumeroColumnaDIFERENCIA) 
    
    TOL=1    
    if SumaTT+TOL >= SumaCA_CN_NR and SumaTT-TOL <= SumaCA_CN_NR:
        Si_o_No = True
       # print(">>> La suma en INFORSLD en DIFERENCIAS filtrada por TT y CA,CN,NR es igual por tanto se puede continuar sin problema")
    else:
        Si_o_No = False
        
    return Si_o_No


def CalculaSiNoCuadraDifereneciasCuadEx (dfs_CUADEX):
    # Vemos si la suma de los valores de la columna diferencia filtrada por
    # TIPO REG = TC es igual a la de TIPO REG = DT
    
    dfs_CUADEX1 = CambiaColumnasPuntos_Y_Comas(dfs_CUADEX,['DIFERENCIA'])
    dfs_CUADEX2 = CambiaColumnas_A_Double(dfs_CUADEX1,['DIFERENCIA'])
    
    DF1 = dfs_CUADEX2
    DF2 = dfs_CUADEX2
    
    DF_filtradoPorTT= DF1.filter((col("TIPO REG") == 'TC') ) 
    
    NumeroColumnaDIFERENCIA = ObtenerNumeroColumna(dfs_CUADEX2,'DIFERENCIA')
    
    SumaTT = SumaValoresColumna(DF_filtradoPorTT,NumeroColumnaDIFERENCIA)
    
    DF_filtradoPorCA_CN_NR = DF2.filter((col("TIPO REG") == "DT")  ) 
          
    SumaCA_CN_NR = SumaValoresColumna(DF_filtradoPorCA_CN_NR,NumeroColumnaDIFERENCIA) 
    
    TOL=1    
    if SumaTT+TOL >= SumaCA_CN_NR and SumaTT-TOL <= SumaCA_CN_NR:
        Si_o_No = True
       # print(">>> La suma en INFORSLD en DIFERENCIAS filtrada por TT y CA,CN,NR es igual por tanto se puede continuar sin problema")
    else:
        Si_o_No = False
    
    return Si_o_No


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

def Devuelve(DFInput,ListaColumnasInput,DFaBuscar,ListaColumnaAbuscar,ListaColumnasSal):
    # A partir de un df de spark (DFInput), 
    # se pueda escoger las columnas que sean (ListaColumnasInput)
    # y que devuelva en formato lista ListaColumnaAbuscar los resultados
    # asociados a una lista de columna de  otro dataframe de spark (DFaBuscar)
    
    # DFInput             -> Dataframe de spark (e.g. df_INFOR)
    # ListaColumnasInput  -> [ "Nº CED","Nº GES", ...]
    # DFaBuscar           -> Dataframe de spark
    # ListaColumnaAbuscar -> ["","",""]
    # ListaSalida         -> 
    
    print("Obteniendo la lista del DFInput a partir de los filtros elegidos")
    my_list=[]
    #Se obtiene, por cada columnainput, una lista con los valores de esa columna
    for columnaelegida in ListaColumnasInput:
        my_list.append(DFInput.select(F.collect_list(columnaelegida)).first()[0]) 
    
    print(my_list)
    # Se busca el resultado en DFaBuscar
    ListaSalida=[]
    for i in range(len(my_list)):    
        df=DFaBuscar.filter(DFaBuscar[ListaColumnaAbuscar[i]].isin(my_list[i]))
    
    df.show()
    #Se obtiene la lista de salida de la columnas elegidas
    for ColumnaSal in ListaColumnasSal:    
        ListaSalida=df.select(F.collect_list(ColumnaSal)).first()[0]
     
    return ListaSalida


def Devuelve2(DFInput,ListaColumnasInput,DFaBuscar,ListaColumnaAbuscar,ListaColumnasSal, NombreFich):
    # A partir de un df de spark (DFInput), 
    # se pueda escoger las columnas que sean (ListaColumnasInput)
    # y que devuelva en formato lista ListaColumnaAbuscar los resultados
    # asociados a una lista de columna de  otro dataframe de spark (DFaBuscar)
    
    # DFInput             -> Dataframe de spark (e.g. df_INFOR)
    # ListaColumnasInput  -> [ "Nº CED","Nº GES", ...]
    # DFaBuscar           -> Dataframe de spark
    # ListaColumnaAbuscar -> ["","",""]
    # ListaSalida         -> 
    
    print("Realizando cruce de datos ", NombreFich)
    ListaValoresColumnaElegida=[] 
    #Se obtiene, por cada columnainput, una lista con los valores de esa columna
    for columnaelegida in ListaColumnasInput:
        ListaValoresColumnaElegida\
            .append(DFInput.select(F.collect_list(columnaelegida)).first()[0]) 
    
   
    
    # Se obtendra en una lista los valores de DFaBuscar de la 
    # ListaColumnaAbuscar
    ListaValoresColumnaAbuscar = []
    listaDatosEnColumnasAbuscar =ListaColumnasSal + ListaColumnaAbuscar
    for columnaelegida in listaDatosEnColumnasAbuscar:
        ListaValoresColumnaAbuscar\
            .append(DFaBuscar.select(F.collect_list(columnaelegida)).first()[0])
    
    
    
    ListaSalida = FiltrarDatosPorListas(ListaValoresColumnaElegida,\
                                        ListaValoresColumnaAbuscar)
       
    return ListaSalida


def FiltrarDatosPorListas(ListaDatos,ListaDatosAfiltrar):
    
    # Se obtiene una lista de los datos filtrados y ordenados de
    # la lista ListaDatosAfiltrar a partir de la ListaDatos

    LongitudLDatos         = len( ListaDatos         ) # n columnas de filtros
    LongitudLDatosFiltrar  = len( ListaDatosAfiltrar ) # n columnas a filtrar+LongitudDatos
    LongitudColumnasSalida = LongitudLDatosFiltrar - LongitudLDatos
    #Lista de datos transpuesta
    ListaDatosTrans = list(map(list, zip(*ListaDatos))) 
    
    #Lista de datos transpuesta
    ListaDatosAfiltrarTrans = list(map(list, zip(*ListaDatosAfiltrar))) 
    
    ListaSalida = []
    
    # Se transforma, si hubiese, los int de las listas a str
    ListaDatosTransStr=ListaDatosTrans
    for i in range(len(ListaDatosTrans)):
        for j in range(len(ListaDatosTrans[0])):
            ListaDatosTransStr[i][j] = str( ListaDatosTrans[i][j] )
    
    for lista in ListaDatosTransStr:              # e.g ["1","4"]   
        ListaEncontrada = 0       
        for lista2 in ListaDatosAfiltrarTrans:    # e.g ["hola",",1","4"]            
            if lista2[-LongitudLDatos:] == lista:               
                ListaSalida.append( lista2[:LongitudColumnasSalida] )
                ListaEncontrada = 1
        if ListaEncontrada == 0:
            # No se a encontrado un match, por tanto no existe
            # el valor buscado en lista2. Esto es un error
            print("No se ha encontrado ", lista, " . A tener en cuenta")
            ListaSalida.append([''])
            #sys.exit(1)
    
    # Se comprueba que la ListaSalida tiene la misma longitud 
    # que la lista
    return ListaSalida


def AnadeColumnaSpark(SQLContext,DFInput,NombreColumna,ListaAInsertar,cambioADouble):
    
    # Se anade una columna a un df de spark a partir de una lista y poniendole
    # un nombre a la columna
    
    #Se crea el dataframe de la Lista a Insertar
    DFListaAInsertar = SQLContext.createDataFrame([(l,) for l in ListaAInsertar], [NombreColumna])
    
    
    #add 'sequential' index and join both dataframe to get the final result
    DFInput          = DFInput         .withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
    DFListaAInsertar = DFListaAInsertar.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

    DFSalida = DFInput.join(DFListaAInsertar, DFInput.row_idx == DFListaAInsertar.row_idx).drop("row_idx")

    if cambioADouble == True:
        DFSalida = DFSalida  .withColumn(NombreColumna, DFSalida[NombreColumna].cast( DecimalType(15,3) ) )

    return DFSalida


def CompruebaFechaLISCONTEN(dfs_LISTCONTEN,Fecha_lab_hoyLISCONTEN):
    # Se comprueba que la fecha que aparece en el nombre del fichero lisconten
    # coincida con la fecha que hay en la columna FECHA PROCESO
    
    #primero se comprueba que en la columna FECHA PROCESO sea siempre la misma
    #fecha
    
    MismaFecha, FechaColumna = DatosColumnaIgual(dfs_LISTCONTEN,"FECHA PROCESO")
    
    if MismaFecha==True:
        print("La fecha de las columnas del fichero LISCONTEN son las mismas")
        
    else:
        print("La fecha de las columnas del fichero LISCONTEN ¡NO! son las mismas")
                  
    if Fecha_lab_hoyLISCONTEN == FechaColumna and\
       MismaFecha             == True:
           
        SiONo=True
    else:
        SiONo=False
    
    return SiONo

def DatosColumnaIgual(DFS,NombreColumna):
    
    #Se comprueba que todos los datos de una columna de un df de spark valgan 
    #lo mismo
    my_list=[]
    my_list.append(DFS.select(F.collect_list(NombreColumna)).first()[0]) 
    
    SiONo=checkList(my_list)
    
    #tomamos un valor de la lista
    FechaColumna=my_list[0][0]
    print(FechaColumna)
    return SiONo,FechaColumna


def checkList(lst): 
  
    #Se chequea que todos los datos de una lista sea el mismo
    ele = lst[0] 
    chk = True
      
    # Comparing each element with first item  
    for item in lst: 
        if ele != item: 
            chk = False
            break; 
              
    return chk


def anadeFechaYEpigrafeADataFrame(SQLContext,DF,fecha_lab_ant,fecha_lab_hoy,epigrafe):
    
    NumFilas=DF.count()
    Fechas=[]
    for i in range(NumFilas):
        Fechas.append(  (fecha_lab_ant,fecha_lab_hoy,i)  )
    
    #NombreColumnasFechas=["Fecha CED","Fecha GES","id"]
    #print(Fechas)
    #FechasDF = SQLContext.createDataFrame(data=Fechas, schema = NombreColumnasFechas)

    DF=DF.withColumn('Fecha CED', lit(fecha_lab_ant))\
         .withColumn('Fecha GES', lit(fecha_lab_hoy))

    #Creamos la columna para el epigrafe, que, 
    #si estamos con INFR-SLD  el epigrafe sera de "Inversion normal"
    #si estamos con LISCONTEN conten el epigrafe sera de "Contensioso"
    #si estamos con LISCONTEN amort  el epigrafe sera de "Amortizado"
    DFnueva=DF.withColumn('Epígrafe', lit(epigrafe)) 
    
    return DFnueva


def BorrarColumnasNullSpark(df):
    """
    This function drops all columns which contain null values.
    :param df: A PySpark DataFrame
    """
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v > 0]
    df = df.drop(*to_drop)
    return df



def FicherosAComprobar(DirectorioFicheros,ListNombresFicheros):
    
    ListComprobacion=[]
    FicherosLista = get_files(DirectorioFicheros)
    NombreExactoFicheros=[]
    
    for NombreBuscado in ListNombresFicheros:
    
        for NombreFicheroBuscando in FicherosLista:
        
            if NombreBuscado in NombreFicheroBuscando:
                
                #print("Fichero ",NombreBuscado," encontrado \n")
                ListComprobacion.append(True)
                NombreExactoFicheros.append(NombreFicheroBuscando)

    return NombreExactoFicheros


def ReformarFecha(Fecha):
    #A partir de Fecha del tipo dd/mm/anno
    #Obtenemos annommdd
    # 17/06/2020 -> 200617
    
    #Comprobacion slashes y longitud de Fecha   
    if  Fecha[2] == "/" and Fecha[5] == "/" and len(Fecha) == 10:
        
        FechaSalida = Fecha[8]+Fecha[9]+Fecha[3]+Fecha[4]+Fecha[0]+Fecha[1]
    else:
        sys.exit("Fallo en reformar fecha a traves de la funcion ReformarFecha")
         
    return FechaSalida


def ReformarFecha2(Fecha):
    # A partir de Fecha del tipo dd/mm/an
    # Obtenemos annommdd
    # 17-06-20 -> 200617
    
    #Comprobacion slashes y longitud de Fecha   
    if  Fecha[2] == "-" and Fecha[5] == "-" and len(Fecha) == 8:
        
        FechaSalida = Fecha[6]+Fecha[7]+Fecha[3]+Fecha[4]+Fecha[0]+Fecha[1]
    else:
        sys.exit("Fallo en reformar fecha a traves de la funcion ReformarFecha2")
         
    return FechaSalida


def ObtieneFechaAPartirDelNombre(NombresCSVencontrados):
    
    fechas     = []
    fechas_lab = []
    
    for NombreCSV in NombresCSVencontrados:
        
        if not "BALANCE" in NombreCSV:
            fechas.append( re.findall('[0-9]', NombreCSV)  )
            
            fechas_lab.append( fechas[-1][4]+fechas[-1][5]    +\
                               '/'                            +\
                               fechas[-1][2]+fechas[-1][3]    +\
                               '/'+'20'                       +\
                               fechas[-1][0]+fechas[-1][1]    ) 
        else:
            FechaBalance = NombreCSV.split(" ")[-1].split(".")[0]
            FechaBalance = re.sub("-","/",FechaBalance)
            
            fechas_lab.append( FechaBalance[0]+FechaBalance[1] +\
                               FechaBalance[2]                 +\
                               FechaBalance[3]+FechaBalance[4] +\
                               FechaBalance[5]+'20'            +\
                               FechaBalance[6]+FechaBalance[7]  ) 
            
    if fechas_lab.count(fechas_lab[0]) == len(fechas_lab):
        print ("Las fechas de todos los ficheros de entrada coinciden \n")            
    else:
        print("Las fechas de algun/todos ficheros de entrada NO coinciden \n")
        sys.exit("La fecha de algun fichero de entrada no coincide")
        
    return fechas_lab[0]


def DevuelveFechaLabAnt(calendar_file, fecha_lab_hoy):
    df_Fechas_laborales = pd.read_excel(calendar_file, #it's not XLSX
                                    keep_default_na=""
                           )
    df_Fechas_laborales.Dia = df_Fechas_laborales.Dia.apply(lambda x: x.strftime('%d/%m/%Y'))

    #Se obtiene cada serie indicando con booleano si es festivo nacional, sabado o domingo
    ffecha_1 = df_Fechas_laborales['Tipo de Festivo']!='Festivo nacional' #solo valen los festivos nacionales??
    ffecha_2 = df_Fechas_laborales['Dia_semana'     ]!='sábado'
    ffecha_3 = df_Fechas_laborales['Dia_semana'     ]!='domingo'


    df_Fechas_laborales = df_Fechas_laborales[ffecha_1 & ffecha_2 & ffecha_3] 

    #Se resetean los indices para mayor comodidad
    df_Fechas_laborales.reset_index(drop=True, inplace=True)

    #Se encuentran los indices de INFOR-SLD.F 

    #index_fecha es para la fecha del fichero 
    #index_lab_ant es la fecha laborar anterior a la fecha del fichero
    index_fecha = df_Fechas_laborales.index[df_Fechas_laborales['Dia'] == fecha_lab_hoy][0]
    fecha_lab_ant = df_Fechas_laborales.iloc[index_fecha-1,0]
        
    return fecha_lab_ant    
    
def CreaDFSpark(sqlContext,server_path,ListaNombres,Fecha,LocalOServer):
    
    dfs=[]
       
    for Nombre in ListaNombres:
                
        pathCompletoFichero=server_path+"\\"+Nombre
        
        print("Creando el dataframe de Spark ",Nombre,end="\r")                   
        dfs.append( sqlContext.read.format('com.databricks.spark.csv')\
                                   .options(header='true',delimiter=';', inferschema='true')\
                                   .load(pathCompletoFichero) )
        print(" \r Creado el dataframe de Spark ",Nombre,"\n",end="")   
      
    return dfs


def CambiaSignoColumnaSapark(sqlContext,DFInput,NombreColumna):
    # Se cambia el signo de una columna "NombreColumna"
    # de un df de spark DFInput . Debe de ser con formato numero int,float,double   
    
    DFOutput = DFInput.withColumn( NombreColumna, - DFInput[NombreColumna] )
    
    return DFOutput


def DevuelveListaColumnaSpark( DFInput, columnaelegida):
    #Devuelve una lista de lo que hay en la columna elegida
    my_list=[]
    #Se obtiene, por cada columnainput, una lista con los valores de esa columna
    
    my_list=(DFInput.select(F.collect_list(columnaelegida)).first()[0]) 
    
    return my_list


def BuscaEnLista( lista1, lista2, boton=1):
    #Compara dos listas y devuelve una lista con los elementos en comun boton=1
    # o los elementos de la lista1 que no aparecen en la lista 2
    comparacion=[] 
    
    if boton == 2:
        
        for dato in lista1:
            if not dato in lista2:
                comparacion.append(dato)
    
    if boton == 1:
        for dato in lista1:
            if dato in lista2:
                comparacion.append(dato)    
                
    return comparacion


def DevuelveDFSparkFiltradoPorLista( DFATratar, ListaColumnas, ColumnaAfiltrar):
    # Devuelve un df de spark filtrado por los valores de ListaColumnas en
    # la columna ColumnaAfiltrar
    return  DFATratar.filter(DFATratar[ColumnaAfiltrar].isin(ListaColumnas))

    
def anadeconcat( dfs_INPUT, nombreNuevaCol, concat1, concat2):
    # Se crea una nueva columna de un df_INPUT con el nombre
    # nombreNuevaCol y los valores concat1,concat2 concatenados  
    def concatenar(string1,string2):    
        return str(string1) + str(string2)
    
    udfValueToCategory = udf(concatenar, StringType())
    
    return dfs_INPUT.withColumn(nombreNuevaCol, udfValueToCategory(concat1,concat2))    


def cogeUltimas_n_Cifras_y_quitaCerosInicio(dfs_Input,nombreCol,n):
    # Se cambia los valores de la columna nombreCol de forma que solo se 
    # obtengan los n ultimos valores y que si los primeros son 0. Se borran
    def get_n_UltimasCifras(string1):
                    
        string1 = str( string1 )
        string2 = string1[-7:]
        
        pattern = r"^0*" #expresion regular para quitar los primeros 0
        
        return re.sub(pattern,"",string2)
        
    udfValueToCategory = udf(get_n_UltimasCifras, StringType())
    
    return dfs_Input.withColumn( nombreCol, udfValueToCategory( nombreCol ) )    
  

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


def anadeFondoNoInformados(sc, sqlContext, df_FondoCuenta, dfsINPUT, ListaFiltros, epigrafe, fecha_laboral_ant, fecha_laboral_hoy, NombDf):
    
    
    print(" Se esta anadiendo los fondos no informados de ", NombDf, "\n")
    # Se saca en una lista las cuentas totales en el FondoCuenta filtrada
    # por ListaFiltros
    df_FondoCuentaFilt = df_FondoCuenta
    ListaDfFiltrado = []
    
    for Filtro in ListaFiltros:
        
        for cadena in Filtro.split(","):
            df_FondoCuentaFilt = df_FondoCuentaFilt[ df_FondoCuentaFilt[ cadena.split("=")[0] ] == cadena.split("=")[1] ] 
            
        ListaDfFiltrado.append( df_FondoCuentaFilt[ df_FondoCuentaFilt[ cadena.split("=")[0] ] == cadena.split("=")[1] ]  )
        df_FondoCuentaFilt = df_FondoCuenta
    
    if len(ListaDfFiltrado) == 1:
        df_FondoCuentaFilt = ListaDfFiltrado[0]
    else:
        
        df_FondoCuentaFilt = ListaDfFiltrado[0]
        for Df in ListaDfFiltrado[1:]:
            df_FondoCuentaFilt = pd.concat([df_FondoCuentaFilt,Df], axis = 0)
            
    
    # Se saca una lista de los nced y nges de lal df de fondoCuenta filtrado
    N_CEDdf_Filtrado = df_FondoCuentaFilt["N_CED"].to_list()
    N_GESdf_Filtrado = df_FondoCuentaFilt["N_GES"].to_list()
    
    NCEDGES_Filtrado = concatenaDosListasString(N_CEDdf_Filtrado,N_GESdf_Filtrado)
   
    # Se obtiene una lista de todos los nombres de dfsINPUT
    N_CEDdfsINPUT = DevuelveListaColumnaSpark( dfsINPUT, "N_CED")
    N_GESdfsINPUT = DevuelveListaColumnaSpark( dfsINPUT, "N_GES")
    
    NCEDGES_dfsINPUT = concatenaDosListasString( N_CEDdfsINPUT, N_GESdfsINPUT )
    
    
    # Vemos las cuentas que NO aparecen en el fichero de entrada dfsINPUT
    NCEDGESNoAparecen = BuscaEnLista( NCEDGES_Filtrado ,\
                                      NCEDGES_dfsINPUT ,\
                                      boton = 2         )
    
    if NCEDGESNoAparecen:
        
        # Se crea el dataframe con las columnas pertinentes
        dfs_FondoCuentaFilt = sqlContext.createDataFrame(df_FondoCuentaFilt)
        
        # Se crea el dataframe con la columna NCEDNGES en el df de spark de
        # la cuenta filtrada.
        dfs_FondoCuentaFilt = anadeconcat(dfs_FondoCuentaFilt,"N_CEDN_GES","N_CED","N_GES")
        
        dfs_FondoCuentaNoAparecen = \
        DevuelveDFSparkFiltradoPorLista( dfs_FondoCuentaFilt ,\
                                         NCEDGESNoAparecen   ,\
                                         "N_CEDN_GES"         )
            
        
        # Se borran las columnas que no son necesarias
        
        columns_to_drop = ['Tipo', 'Entidad','Disponible']
        
        dfs_FondoCuentaNoAparecen = dfs_FondoCuentaNoAparecen.drop( *columns_to_drop )
        
        
        # Se anade las columnas EXIS CED EXIS GES Y DIFERENCIA
    
        NumRows = dfs_FondoCuentaNoAparecen.count()
        
        ColumnasToCero = ["EXIS CED", "EXIS GES", "DIFERENCIA"]
        
        for Columna in ColumnasToCero:
            
            dfs_FondoCuentaNoAparecen = AnadeColumnaSpark( sqlContext                ,\
                                                           dfs_FondoCuentaNoAparecen ,\
                                                           Columna                   ,\
                                                           ['0'] * NumRows           ,\
                                                           True                       )
        # Se anade las fechas y el epigrafe
    
        dfs_FondoCuentaNoAparecen = anadeFechaYEpigrafeADataFrame( sqlContext                ,\
                                                                   dfs_FondoCuentaNoAparecen ,\
                                                                   fecha_laboral_ant         ,\
                                                                   fecha_laboral_hoy         ,\
                                                                   epigrafe                   )
        
        # Reordena
        dfs_FondoCuentaNoAparecen = dfs_FondoCuentaNoAparecen.select([
                                                                          "Fecha CED"         ,\
                                                                          "Fecha GES"         ,\
                                                                          "N_CED"             ,\
                                                                          "N_GES"             ,\
                                                                          "Nombre_abreviado"  ,\
                                                                          "Epígrafe"          ,\
                                                                          "EXIS GES"          ,\
                                                                          "EXIS CED"          ,\
                                                                          "DIFERENCIA"        ])    
    else:
    
        field = [StructField( "Fecha CED"        , StringType (), True ) ,\
                 StructField( "Fecha GES"        , StringType (), True ) ,\
                 StructField( "N_CED"            , IntegerType(), True ) ,\
                 StructField( "N_GES"            , IntegerType(), True ) ,\
                 StructField( "Nombre_abreviado" , StringType (), True ) ,\
                 StructField( "Epígrafe"         , StringType (), True ) ,\
                 StructField( "EXIS GES"         , DecimalType (15,3), True ) ,\
                 StructField( "EXIS CED"         , DecimalType (15,3), True ) ,\
                 StructField( "DIFERENCIA"       , DecimalType (15,3), True )  ]
            
        schema = StructType(field)
        dfs_FondoCuentaNoAparecen = sqlContext.createDataFrame(sc.emptyRDD(), schema)
             
        lista = [['0','0','0','0','','0','0','0','0']]
        lista_df= sqlContext.createDataFrame(lista)
        dfs_FondoCuentaNoAparecen = dfs_FondoCuentaNoAparecen.union(lista_df)           
    
    return dfs_FondoCuentaNoAparecen
    

def concatenaDosListasString(lista1,lista2):
    
    ListaSalida = []
    i = 0
    if len(lista1) == len(lista2):
        
        for valor in lista1:           
            ListaSalida.append( str(valor) + str(lista2[i]) )          
            i=i+1
                        
    else:
        print("Las listas en el metodo de concatenar string no son iguales")
        
    return ListaSalida
        
def creaDfPandasBalances(server_path,NombresCSVencontrados):
    
    DictBalances = { "N_GES":[], "df":[] }
    
    for fichero in NombresCSVencontrados:
        if "BALANCE" in fichero:
            NombreFichBalance = fichero
            break
    
   
    path = server_path + '\\' + NombreFichBalance
    x1 = pd.ExcelFile(path)
    
    ListaSheet = x1.sheet_names
    
    for Sheet in ListaSheet:
        DictBalances["N_GES"].append( Sheet           )
        DictBalances["df"   ].append( x1.parse(Sheet) )
                   
    return DictBalances

def computaBalances( sc, sqlContext, unionDFBalances, df_FondoCuenta, DictBalances):
    
    
    #ListaDfBalances = DictBalances["df"]
    Lista_N_GES     = DictBalances["N_GES"]
    
    dfsBalances = creaDFSparkBalances( sqlContext, Lista_N_GES, DictBalances, df_FondoCuenta)

    # 
    columns_to_dropUnionBalances = [ 'EXIS CED', 'DIFERENCIA' ]    
    unionDFBalances = unionDFBalances.drop(*columns_to_dropUnionBalances)
    
    unionDFBalances = unionDFBalances.join(dfsBalances, on='N_GES', how='inner')
   
    def eligeEnTabla(epigrafe, col1, col2, col3, col4):
        if epigrafe == "Inversión normal":
            return col1
        if epigrafe == "Contencioso":
            return col2
        if epigrafe == "Amortizado":
            return col3       
        if epigrafe == "Impagado":
            return col4

    
    udfeligeEnTabla = udf(eligeEnTabla, DecimalType(15,3))
    
    unionDFBalances= unionDFBalances.withColumn("CONT GES", udfeligeEnTabla( "Epígrafe" ,\
                                                                             "InvNorm"  ,\
                                                                             "Content"  ,\
                                                                             "Amortiz"  ,\
                                                                             "Impagado"  ))   
    
    def calculaDif(exisges, partced):
        return exisges + partced
    
    udfcalculaDif = udf(calculaDif, DecimalType(15,3))
    
    unionDFBalances= unionDFBalances.withColumn("DIFERENCIA", udfcalculaDif( "EXIS GES" ,\
                                                                             "CONT GES"  ))   
    
    unionDFBalances = unionDFBalances.select([ "Fecha CED"          ,\
                                               "Fecha GES"          ,\
                                               "N_GES"              ,\
                                               "Nombre_abreviado"   ,\
                                               "Epígrafe"           ,\
                                               "EXIS GES"           ,\
                                               "CONT GES"           ,\
                                               "DIFERENCIA"]         )

    
    # Se borran las filas que tengan amortizado y impagado
    
    unionDFBalances = unionDFBalances.filter( unionDFBalances["Epígrafe"] != 'Amortizado' )\
                                     .filter( unionDFBalances["Epígrafe"] != 'Impagado'   )
    
    return unionDFBalances
    
    
def creaDFSparkBalances( sqlContext, Lista_N_GES, DictBalances, df_FondoCuenta):
    
    
    Datos = []
    i=0
    
    # Se comprueba que en las pestanas solo hayan valores numericos
    Lista_N_GES = list(filter(lambda x: x.isnumeric(), Lista_N_GES))
    
    for N_GES in Lista_N_GES:
        N_GESnumber = int(str(N_GES))
        
        if str(N_GESnumber) in df_FondoCuenta["N_GES"].to_list():
            
            dato1 = sumaInverNormalBalance(N_GESnumber, DictBalances["df"][i], df_FondoCuenta)
            dato2 = sumaContensioso       (N_GESnumber, DictBalances["df"][i], df_FondoCuenta)
        else:
            print("El fondo: ", N_GES, "captado del fichero Balances no se encuentra en fondo balances")
            dato1 = 0.0
            dato2 = 0.0
        dato3 = 0.0
        dato4 = 0.0
        dato0 = N_GESnumber
        
        Datos.append([dato0,dato1,dato2,dato3,dato4])
        i = i+1
        
    # Crea DataFrame de spark para DictBalance    
    
    field = [StructField( "N_GES"   , IntegerType(), True ),\
             StructField( "InvNorm" , StringType (), True ),\
             StructField( "Content" , StringType (), True ),\
             StructField( "Amortiz" , StringType (), True ),\
             StructField( "Impagado", StringType (), True ),]            
        
    dfoutput = sqlContext.createDataFrame(Datos, StructType(field))
    
    dfoutput =  dfoutput.withColumn("N_GES"    , dfoutput["N_GES"    ].cast( IntegerType (    ) ) )\
                        .withColumn("InvNorm"  , dfoutput["InvNorm"  ].cast( DecimalType (15,3) ) )\
                        .withColumn("Content"  , dfoutput["Content"  ].cast( DecimalType (15,3) ) )\
                        .withColumn("Amortiz"  , dfoutput["Amortiz"  ].cast( DecimalType (15,3) ) )\
                        .withColumn("Impagado" , dfoutput["Impagado" ].cast( DecimalType (15,3) ) )
    
    return dfoutput
    

def  sumaInverNormalBalance(N_GES, dfpandas, df_FondoCuenta):
    
    
    Tipo = df_FondoCuenta[df_FondoCuenta["N_GES"] == str(N_GES)]["Tipo"]
    suma = "0.000"
    
    if Tipo.to_string().split(" ")[-1] == "Préstamos":
        
        Asumar = ["N  300","N  305","N  310","N  315","N  320","N  325","N  429"]
        
        
        for Num in Asumar:
            
            if Num in dfpandas["Unnamed: 0"].to_list():             
                suma = sumaString(dfpandas[dfpandas["Unnamed: 0"]==Num]["Unnamed: 4"].to_list()[0],suma  )
        
    
    if Tipo.to_string().split(" ")[-1] == "Créditos":
        
        Asumar = ["N  330","N  331"]
        
        for Num in Asumar:
            if Num in dfpandas["Unnamed: 0"].to_list(): 
                suma = sumaString(dfpandas[dfpandas["Unnamed: 0"]==Num]["Unnamed: 4"].to_list()[0],suma  )
    return suma

def  sumaContensioso(N_GES, dfpandas, df_FondoCuenta):
    
    Tipo = df_FondoCuenta[df_FondoCuenta["N_GES"] == str(N_GES)][["Tipo"]]
    suma = 0.000
    
    if Tipo.to_string().split(" ")[-1] == "Préstamos":
        
        Asumar = ["N  426","N  428"]
               
        for Num in Asumar:
            if Num in dfpandas["Unnamed: 0"].to_list(): 
                suma = sumaString(dfpandas[dfpandas["Unnamed: 0"]==Num]["Unnamed: 4"].to_list()[0],suma  )
    if Tipo.to_string().split(" ")[-1] == "Créditos":
        
        Asumar = ["N  427","N  430"]
        
        for Num in Asumar:
            if Num in dfpandas["Unnamed: 0"].to_list(): 
                suma = sumaString(dfpandas[dfpandas["Unnamed: 0"]==Num]["Unnamed: 4"].to_list()[0],suma  )
               
    return suma

def sumaString(SrtA, StrB):
    return str( float(SrtA) + float(StrB) )



def convierteListaNumaString(lista):
    listOutput = []
    
    for num in lista:
        listOutput.append(str(num))
    
    return listOutput
        
def ObtieneListaEpifrafes(df_FondoCuenta, N_GES, N_CED, fechas):
        
    ListaEpigrafe = []
    ListaEpigrafe.append("Inversión normal")
        
    Tipo             = df_FondoCuenta[ df_FondoCuenta["N_GES"] == str(N_GES) ]["Tipo"            ].to_list()[0]
    Entidad          = df_FondoCuenta[ df_FondoCuenta["N_GES"] == str(N_GES) ]["Entidad"         ].to_list()[0]
    Nombre_abreviado = df_FondoCuenta[ df_FondoCuenta["N_GES"] == str(N_GES) ]["Nombre_abreviado"].to_list()[0]
    if Entidad == "Santander":
        ListaEpigrafe.append("Contencioso")
        ListaEpigrafe.append("Amortizado")            
        if Tipo == "Préstamos":
            ListaEpigrafe.append("Impagados")
            
    if Entidad == "UCI" or Entidad == "SCF":
        ListaEpigrafe.append("Impagados")
    
    ListaSalida = []
    
    for Datos in ListaEpigrafe:
        lista = []
        lista.append(fechas[0])
        lista.append(fechas[1])
        lista.append(str(N_CED))
        lista.append(str(N_GES))
        lista.append(Nombre_abreviado)
        lista.append(Datos)
        lista.append("0.000")
        lista.append("0.000")
        lista.append("0.000")
        
        ListaSalida.append(lista)
        
    return ListaSalida


def esqueletoInformeDiarioExistencias(sqlContext, df_FondoCuenta, fechas):
    
    assert len(fechas) == 2
    
    List_N_GES = df_FondoCuenta["N_GES"].to_list()
    List_N_CED = df_FondoCuenta["N_CED"].to_list()   
    
    assert len(List_N_GES) == len(List_N_CED)
    
    ListaEpigrafes = []
    for N_GES, N_CED in zip(List_N_GES, List_N_CED):       
        ListaEpigrafes.append( ObtieneListaEpifrafes(df_FondoCuenta, N_GES, N_CED,fechas) )
    
    ListaEpigrafesMod = []  
    for Lista in ListaEpigrafes:
        for l in Lista:
            ListaEpigrafesMod.append(l)
    
    
    field = StructType(\
            [ StructField( "Fecha_CED"        , StringType (), True ),\
              StructField( "Fecha_GES"        , StringType (), True ),\
              StructField( "N_CED"            , StringType (), True ),\
              StructField( "N_GES"            , StringType (), True ),\
              StructField( "Nombre_Abreviado" , StringType (), True ),\
              StructField( "Epígrafe"         , StringType (), True ),\
              StructField( "EXIS_GES"         , StringType (), True ),\
              StructField( "EXIS_CED"         , StringType (), True ),\
              StructField( "DIFERENCIA"       , StringType (), True ) ]  )          

    
    dfoutput = sqlContext.createDataFrame(data = ListaEpigrafesMod, schema = field)
    
    # dfoutput =  dfoutput.withColumn("N_GES"    , dfoutput["N_GES"    ].cast( IntegerType (    ) ) )\
    #                     .withColumn("InvNorm"  , dfoutput["InvNorm"  ].cast( DecimalType (15,3) ) )\
    #                     .withColumn("Content"  , dfoutput["Content"  ].cast( DecimalType (15,3) ) )\
    #                     .withColumn("Amortiz"  , dfoutput["Amortiz"  ].cast( DecimalType (15,3) ) )\
    #                     .withColumn("Impagado" , dfoutput["Impagado" ].cast( DecimalType (15,3) ) )
    
    
    return dfoutput












