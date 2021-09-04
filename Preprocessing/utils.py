import pandas as pd
import numpy as np

def replace_log(data, col, col_id, map, log):
  '''
  Toma la serie 'data[col]' del dataframe 'data' y le aplica los reemplazos 
  del diccionario 'map' cuando aplique, identificando cada cambio con el 
  respectivo valor de la columna 'data[col_id]' para reportarlo a la lista 'log'
  con la estructura:
  "'col_id' = identificador, 'col': valor_inicial >> valor_final"
  Output: data[col].replace(map, regex=True)
    La serie 'data[col]' con los cambios hechos
  '''
  new_col = data[col].replace(map, regex=True)
  temp = pd.DataFrame({'id': data[col_id], 'old': data[col], 'new': new_col}).dropna(how='any')
  for index, row in temp.query(f'old != new').iterrows():
    log.append(f"'{col_id}' = {row['id']}, '{col}': {row['old']} >> {row['new']}")
  return new_col

def impute_nans(value, nan_values):
  import numpy as np
  '''
  Si el valor 'value' se encuentra en el conjunto 'nan_values', regresa un NaN.
  De lo contrario, regresa el mismo valor.
  '''
  if value in nan_values:
    return np.NaN
  else:
    return value


def impute_nans_range(value, range):
  import numpy as np
  '''
  Si el valor 'value' no se encuentra en el intervalo 'range', regresa un NaN.
  De lo contrario, regresa el mismo valor.
  '''
  if (value < range[0]) or (value > range[1]):
    return np.NaN
  else:
    return value

def impute_year(date, new_year):
  '''
  Reemplaza la fecha 'date' por una del año 'new_year', respetando mes y día.
  Por ejemplo:
  Input: date = 1900-10-11, new_year = 2017
  Output: 2017-10-11
  '''
  try:
    new_date = pd.to_datetime(f'{new_year} - {date.month} - {date.day}')
  except:
    # Esta excepción previene generar el 29-feb en años no bisiestos
    new_date = pd.to_datetime(f'{new_year} - {date.month} - {date.day - 1}')
  return new_date

def impute_nans_range_log(data, col, col_id, range, log):
  '''
  Toma la serie 'data[col]' del dataframe 'data' y le imputa un valor NaN cuando
  se encuentre por fuera del intervalo [range[0], range[1]], identificando cada 
  cambio con el respectivo valor de la columna 'data[col_id]' para reportarlo a 
  la lista 'log' con la estructura:
  "'col_id' = identificador, 'col': valor_inicial >> nan"
  Output: data[col].replace(map, regex=True)
    La serie 'data[col]' con los cambios hechos
  '''
  new_col = data[col].apply(lambda x: impute_nans_range(x, range))
  temp = pd.DataFrame({'id': data[col_id], 'old': data[col], 'new': new_col}).\
    dropna(subset=['old'])
  for index, row in temp.query(f'old != new').iterrows():
    log.append(f"'{col_id}' = {row['id']}, '{col}': {row['old']} >> {row['new']}")
  return new_col

def impute_years_log(data, col, col_id, col_year, log):
  '''
  Toma la serie de fechas 'data[col]' del dataframe 'data' y le imputa una del
  año en la columna 'col_year', respetando mes y día, identificando cada cambio con el 
  respectivo valor de la columna 'data[col_id]' para reportarlo a la lista 'log'
  con la estructura:
  "'col_id' = identificador, 'col': valor_inicial >> valor_final"
  Output: data[col].replace(map, regex=True)
    La serie 'data[col]' con los cambios hechos
  '''
  new_col = data[[col_year, col]].apply(lambda x: impute_year(x[col], int(x[col_year])),
                                        axis='columns')
  temp = pd.DataFrame({'id': data[col_id], 'old': data[col], 'new': new_col}).dropna(how='any')
  for index, row in temp.query(f'old != new').iterrows():
    log.append(f"'{col_id}' = {row['id']}, '{col}': {row['old']} >> {row['new']}")
  return new_col


def replace_year(date, new_year, old_year):
  if date.year == old_year:
    return impute_year(date, new_year)
  else:
    return date


def concatenar_codigos(x):
  '''Toma el primer caracter de cada código de categoría (número entero >= -1) y los concatena
  Dado que los NaNs se codifican originalmente como -1, son convertidos en - en la concatenación
  Ejemplo: [0, -1, 2, 1] --> "0-21"
  '''
  return ''.join(x.astype('str').str[0])