import pandas as pd
import numpy as np
import lxml.etree as ET
def extract(): # Función que extrae los datos
    order_details = pd.read_csv('order_details.csv', encoding='latin1', sep=';') # Leo el archivo order_details.csv
    orders = pd.read_csv('orders.csv', encoding='latin1',sep=';') # Leo el archivo orders.csv
    pizza_types = pd.read_csv('pizza_types.csv',encoding='latin1',sep=',') # Leo el archivo pizza_types.csv
    return  order_details, orders, pizza_types
def analisis_nulls(order_details, orders, pizza_types): # Función que analiza los nulls
    analisis = [order_details, orders, pizza_types] # Creo una lista con los dataframes
    for df in analisis:
        df.info() # Imprimo la información de cada dataframe
        print(df.isnull().sum()) # Imprimo la cantidad de nulls de cada dataframe
    return
def limpiar_datos(order_details, orders):
    ord = pd.merge(order_details, orders, on='order_id') # Hago un merge de los dataframes order_details y orders
    #quito columna time
    ord = ord.drop(columns=['time'])
    ord = ord.dropna() # Elimino los nulls
    pd.set_option('mode.chained_assignment', None) # Deshabilito el warning SettingWithCopyWarning
    for i in range(1,len(ord['pizza_id'])):
        try:
            m = ord['pizza_id'][i]
            palabra = ''
            for l in range(len(m)):
                if m[l] =='@':
                    palabra += 'a' # Reemplazo las @ por a
                elif m[l] == '0':
                    palabra += 'o' # Reemplazo los 0 por o
                elif m[l] == '-':
                    palabra += '_' # Reemplazo los - por _
                elif m[l] == ' ':
                    palabra += '_' # Reemplazo los espacios por _
                elif m[l] == '3':
                    palabra += 'e' # Reemplazo los 3 por e
                else:
                    palabra += m[l] # Si no es ninguna de las anteriores, la agrego tal cual
            ord['pizza_id'][i] = ord['pizza_id'][i].replace(m,palabra)  # Reemplazo la palabra por la nueva
        except:
            #elimino la fila que no se pudo limpiar
            try:
                ord = ord.drop(i)
            except:
                pass
    order_details_2 = ord[['order_details_id','order_id', 'pizza_id', 'quantity']] # Creo un nuevo dataframe con las columnas que me interesan
    order_details_2.to_csv('order_details_2.csv', index=False) # Guardo el dataframe en un archivo csv
    orders_2 = ord[['order_id','date']] # Creo un nuevo dataframe con las columnas que me interesan
    orders_2.to_csv('orders_2.csv', index=False) # Guardo el dataframe en un archivo csv
    return order_details_2, orders_2
def transform(order_details, orders, pizza_types):
    pizza = {}
    fechas = []
    for i in range(len(pizza_types)):
        pizza[pizza_types['pizza_type_id'][i]] = pizza_types['ingredients'][i] #guardo los ingredientes en un diccionario
    for fecha in orders['date']:
        try: 
            f = pd.to_datetime(float(fecha)+3600, unit='s') #convierto la fecha a datetime
        except:
            f = pd.to_datetime(fecha) # Convierto la fecha a datetime
        fechas.append(f) #guardo las fechas en una lista
    cant_pedidos = [[] for _ in range(53)] #creo una lista de listas para guardar la cantidad de pedidos por semana
    pedidos = [[] for _ in range(53)] #creo una lista de listas para guardar los pedidos por semana
    for pedido in range(len(fechas)):
        # print(fechas[pedido])
        cant_pedidos[fechas[pedido].week-1].append(pedido+1) #guardo la cantidad de pedidos por semana
    bucle = 0
    for p in range(2,len(order_details['order_details_id'])): 
        try:
            bucle = abs(order_details['quantity'][p])
        except:
            try:
                if order_details['quantity'][p] == 'One' or order_details['quantity'][p] == 'one':
                    bucle = 1
                elif order_details['quantity'][p] == 'Two' or order_details['quantity'][p] == 'two':
                    bucle = 2
            except:
                pass
        try:
            for i in range(bucle):
                pedidos[fechas[abs(order_details['order_id'][p]-1)].week-1].append(order_details['pizza_id'][p]) #guardo los pedidos por semana teniendo en cuenta la cantidad de pizzas
        except:
            pass
    ingredientes_anuales = {}
    diccs = []
    for dic in range(53):
        diccs.append({}) #creo una lista de diccionarios para guardar los ingredientes por semana
    for i in range(len(pizza_types)):
        ingreds = pizza_types['ingredients'][i] #guardo los ingredientes en una variable
        ingreds = ingreds.split(', ') #separo los ingredientes
        for ingrediente in ingreds:
            ingredientes_anuales[ingrediente] = 0
            for i in range(len(diccs)):
                diccs[i][ingrediente] = 0 #guardo los ingredientes en los diccionarios
    for i in range(len(pedidos)):
        for p in pedidos[i]:
            ing = 0
            tamano = 0
            if p[-1] == 's': #guardo el tamaño de la pizza
                ing = 1 #si es s la pizza tiene 1 ingrediente de cada
                tamano = 2 
            elif p[-1] == 'm':
                ing = 2 #si es m la pizza tiene 2 ingredientes de cada
                tamano = 2
            elif p[-1] == 'l':
                if p[-2] == 'x':
                    if p[-3] == 'x':
                        ing = 5 #si es xxl la pizza tiene 5 ingredientes de cada
                        tamano = 4
                    else:
                        ing = 4 #si es xl la pizza tiene 4 ingredientes de cada
                        tamano = 3
                else:
                    ing = 3 #si es l la pizza tiene 3 ingredientes de cada
                    tamano = 2
            ings = pizza[p[:-tamano]].split(', ')
            for ingrediente in ings:
                ingredientes_anuales[ingrediente] += ing #guardo los ingredientes en el diccionario de ingredientes anuales
                diccs[i][ingrediente] += ing #guardo los ingredientes en los diccionarios de ingredientes por semana
    for i in range(len(diccs)):
        for j in diccs[i]:
            diccs[i][j] = int(np.ceil((diccs[i][j] + (ingredientes_anuales[j]/53))/2)) #aplico la predicción
    return diccs
def load(diccs,order_details, orders, pizza_types):
    root = ET.Element('root')  #creo el elemento root
    for i in range(len(diccs)):
        semana = ET.SubElement(root, 'semana') #creo el subelemento semana
        semana.set('numero', str(i+1)) #le asigno un número a cada semana
        for j in diccs[i]:
            ingrediente = ET.SubElement(semana, 'ingrediente') #creo el subelemento ingrediente
            ingrediente.set('nombre', j) #nombre del ingrediente
            ingrediente.text = str(diccs[i][j]) #número de ingredientes
        prediccion = ET.SubElement(semana, 'prediccion') #creo el subelemento prediccion
        prediccion.text = 'La predicción se basa en que la cantidad de ingredientes semanales que se necesitarán en una semana serán aproximadamente la media entre los ingredientes usados esa semana en el 2015 y los ingredientes usados en el 2015 entre las 53 semanas.' #cantidad de ingredientes
    for o in order_details.columns:
        order = ET.SubElement(root, 'categorias') #creo el subelemento categorias
        order.set('categoria', str(o)) 
        order.text = str(order_details[o].dtype)
    for o in orders.columns:
        order = ET.SubElement(root, 'categorias')
        order.set('categoria', str(o))
        order.text = str(orders[o].dtype)
    for o in pizza_types:
        order = ET.SubElement(root, 'categorias')
        order.set('categoria', str(o))
        order.text = str(pizza_types[o].dtype)
    tree = ET.ElementTree(root) #creo el árbol
    tree.write('pizzas.xml', pretty_print=True) #guardo el árbol en un xml, pretty_print=True para que se vea más ordenado
    return
if __name__ == '__main__':
    order_details, orders, pizza_types = extract()
    analisis_nulls(order_details, orders, pizza_types)
    order_details, orders = limpiar_datos(order_details,orders)
    diccs = transform(order_details, orders, pizza_types)
    load(diccs,order_details, orders, pizza_types)