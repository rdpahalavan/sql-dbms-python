import sys
import sqlparse
from sql_metadata import Parser

def read_content():
  with open(sql_file_path, 'r') as f:
    contents = f.read()

  statements = sqlparse.split(contents)
  n_statements = len(statements)

  parsed = sqlparse.parse(contents)

  create_statement_count = 0
  for i in range(len(statements)):
    statement_type = sqlparse.sql.Statement(parsed[i]).get_type()
    if(statement_type=='CREATE'):
      create_statement_count+=1;

  if len(custom_select_query) > 5:
    statements[create_statement_count] = custom_select_query

  create_statement(statements, create_statement_count)
  select_statement(statements, n_statements, create_statement_count)

class create:
  
  def __init__(self, create_statement):
    self.statement = sqlparse.format(create_statement, reindent=True, keyword_case='upper')
    self.columns_raw = self.parser().columns
    self.table_name = self.parser().tables[0]
    self.columns = self.columns()
    self.column_types = self.column_types()

  def parsed(self):
    parsed = sqlparse.parse(self.statement)[0]
    return parsed
    
  def parser(self):
    parser = Parser(self.statement)
    return parser

  def columns(self):
    column_names = self.parser().columns
    column_names = [self.table_name + '.' + name for name in column_names]
    return column_names

  def column_types(self):
    column_types = []
    type_finder = self.statement.split('(')[1]
    type_finder = type_finder.split(',')
    for j in self.columns_raw:
      for k in type_finder:
        if j in k:
          column_type = k
          break
      if 'int' in column_type:
        column_types.append('int')
      elif 'float' in column_type or 'double' in column_type:
        column_types.append('float')
      elif 'date' in column_type:
        column_types.append('date')
      else:
        column_types.append('string')
    return column_types

def create_statement(statements, create_statement_count):
  global column_type_list;
  global column_name_list;
  global create_dict;

  create_dict = {}
  column_type_list = []
  column_name_list = []

  for i in range(create_statement_count):
    create_dict[i] = create(statements[i])
    column_type_list.append(create_dict[i].column_types)
    column_name_list.append(create_dict[i].columns)

  column_type_list = [item for sublist in column_type_list for item in sublist]
  column_name_list = [item for sublist in column_name_list for item in sublist]

class select:
  
  def __init__(self, select_statement):
    self.statement = sqlparse.format(select_statement, reindent=True, keyword_case='upper')
    self.columns = self.parser().columns_dict
    self.join_column_list = []
    self.table_alias = self.parser().tables_aliases
    self.where_statement = self.where()
    self.table_names = self.parser().tables
    self.select_columns = self.select_columns()
    self.is_aggregate = False
    self.select_function = self.select_function()
    self.is_subquery = False
    self.select_subquery_list = self.select_subquery()
    self.is_join = False
    self.join_statement = self.join()
    self.column_header = self.column_headers()
    self.limit = self.get_limit()

  def parsed(self):
    parsed = sqlparse.parse(self.statement)[0]
    return parsed
    
  def parser(self):
    parser = Parser(self.statement)
    return parser

  def select_columns(self):

    columns = self.statement.split('FROM')[0]
    if '*' in columns:
      columns_list = column_name_list
    else:
      columns_list = self.columns['select']
      columns_list = self.change_column_names(columns_list)
      
    for j in self.table_names:
        for k in column_name_list:
          if j in k:
            self.join_column_list.append(k)
    return columns_list

  def select_function(self):

    functions = ['+', '-', '*', '/', '%', 'SUM', 'MIN', 'MAX', 'AVG', 'COUNT']

    columns = self.statement.split('FROM')[0]
    columns = columns.replace('SELECT', '').strip()
    
    if any(agg in columns for agg in functions) and len(columns) > 3 and '.*' not in columns:
      self.is_aggregate = True

    columns = columns.replace('\n', '')
    columns = columns.replace(' ', '')

    columns_list = columns.split(',')
    columns_list = self.change_column_function(columns_list)

    return columns_list

  def where(self):
    where = ';'
    num_tokens = len(self.parsed().tokens)
    for i in reversed(range(num_tokens)):
      if 'WHERE' in str(self.parsed()[i]):
        where = str(self.parsed()[i])
        break

    if len(where)<5:
      where="1==1"
      return where
    
    if '.' not in where or len(self.table_alias.values()) != len(set(self.table_alias.values())):
      for j in column_name_list:
        col_name = j.split('.')[1]
        if col_name in where and where[where.find(col_name)-1] != '.':
          where = where.replace(col_name, j)

    where = self.process_condition(where)
    return where

  def change_column_names(self, columns_list):
    for i in range(len(column_name_list)):
        for j in range(len(columns_list)):
          if columns_list[j] in column_name_list[i]:
            columns_list[j] = columns_list[j].replace(columns_list[j], column_name_list[i])
    return columns_list

  def change_column_function(self, columns_list):
    try:
      select_alias = self.parser().columns_aliases_dict['select']
      is_alias = True
    except:
      is_alias = False
    duplicate=[]
    for i in range(len(column_name_list)):
        for j in range(len(columns_list)):
          flag = 0
          if is_alias:
            for alias in select_alias:
              if alias in columns_list[j]:
                columns_list[j] = columns_list[j].replace('AS'+alias, '')
          if column_name_list[i].split('.')[1] in columns_list[j]:
            if column_name_list[i].split('.')[1]+str(j) in duplicate:
              continue
            if(columns_list[j].find('(')!=-1):
              first_part = columns_list[j][0:columns_list[j].find('(')]
              second_part = columns_list[j][columns_list[j].find('('):]
            else:
              first_part = ''
              second_part = columns_list[j]
            if '.' in columns_list[j] and 'rec' not in columns_list[j]:
              if column_name_list[i] in columns_list[j]:
                flag = 1
              columns_list[j] = second_part.replace(column_name_list[i], 'rec["'+column_name_list[i]+'"]')
            else:
              columns_list[j] = second_part.replace(column_name_list[i].split('.')[1], 'rec["'+column_name_list[i]+'"]')
              flag = 1
            columns_list[j] = first_part + columns_list[j]
            
          if flag == 1:
            duplicate.append(column_name_list[i].split('.')[1]+str(j))
          if len(self.table_alias) > 0:
            for alias in self.table_alias:
                columns_list[j] = columns_list[j].replace(alias+'.', '')
    return columns_list

  def get_limit(self):
    limit = self.parser().limit_and_offset
    if limit is None:
      limit = -1
    else:
      limit = limit[0]
    return limit

  def column_headers(self):
    columns = self.statement.split('FROM')[0]
    columns = columns.replace('SELECT', '').strip()
    if '*' in columns and not self.is_aggregate:
      header_list = []
      for i in self.table_names:
        for j in column_name_list:
          if i in j:
            if len(self.table_names)==1:
              header_list.append(j.split('.')[1])
            else:
              header_list.append(j)
      return header_list
    else:
      columns = columns.split(',')
      for i in range(len(columns)):
        columns[i] = columns[i].strip()
        if 'AS ' in columns[i]:
          columns[i] = columns[i].split('AS ')[1].strip()
        if len(self.table_names)==1:
          if '.' in columns[i]:
            columns[i] = columns[i].split('.')[1]
      return columns

  def select_subquery(self):
    subqueries = self.parser().subqueries
    select_subquery_result = []
    if(len(subqueries)>0):
      self.is_subquery = True
      for i in select_columns_list:
        for j in select_function:
          if i in j: select_subquery_result.append(i)
    return select_subquery_result

  def process_condition(self, condition):
    condition = condition.replace('WHERE ', '')
    condition = condition.replace('ON ', '')
    if len(self.table_alias) > 0:
      for i in self.table_alias:
        condition = condition.replace(i, self.table_alias[i])
    for i in column_name_list:
      condition = condition.replace(i, 'rec["'+i+'"]')
    condition = condition.replace('=', '==')
    condition = condition.replace('\n', '')
    condition = condition.replace('AND', 'and')
    condition = condition.replace('OR', 'or')
    condition = condition.replace(',', 'or')
    condition = condition.replace(';', '')
    return condition

  def join(self):
    join = ''
    if self.columns is not None and 'join' in self.columns:
      self.is_join = True
      join = self.statement[self.statement.find('ON'):self.statement.find('WHERE')]
      join = join.split('ON')
      for i in range(len(join)-1):
        join[i] = join[i][:join[i].find('INNER')]
        join[i] = join[i][:join[i].find('JOIN')]
      join.pop(0)
      join = 'AND'.join(str(x) for x in join)
      join = self.process_condition(join)
    return join

def select_statement(statements, n_statements, create_statement_count):
  global select_dict;

  select_dict = {}
  for i in range(n_statements - create_statement_count):
    select_dict[i] = select(statements[create_statement_count + i])

def convert_to_type(record):
  for i in range(len(record)):
    if column_type_list[i] == 'int':
      record[i] = int(record[i])
    elif column_type_list[i] == 'float':
      record[i] = float(record[i])
    else:
      record[i] = str(record[i])
  return record

def print_aggregate_result(function, array):
  if(len(array[0])>0):
    for i in range(len(function)):
      if 'SUM' in function[i]:
        val = sum(array[i])
      elif 'AVG' in function[i]:
        val = sum(array[i])/len(array[i])
      elif 'MIN' in function[i]:
        val = min(array[i])
      elif 'MAX' in function[i]:
        val = max(array[i])
      elif 'COUNT' in function[i]:
        val = len(array[i])
      if i == len(function)-1:
        print(val)
      else:
        print(val, end='|')

def LIMIT(rec):
  global limit;
  if limit==-1 or is_aggreagate:
    PROJECTION(rec)
  else:
    limit=limit-1
    if limit>=0:
      PROJECTION(rec)
    else:
      return True

def AGGREGATION(rec):
  for i in range(len(select_function)):
    if any(agg in select_function[i] for agg in ['SUM', 'MIN', 'MAX', 'AVG', 'COUNT']):
      if '(*)' in select_function[i]:
        final_list[i].append(rec)
      else:
        final_list[i].append(eval(select_function[i][select_function[i].find('('):]))
    elif i < len(select_function)-1:
      print(eval(select_function[i]), end='|')
    else:
      print(eval(select_function[i]))

def PROJECTION(record_dict):
  rec = dict((k, record_dict[k]) for k in select_columns_list if k in record_dict)
  if is_aggreagate == False and is_subquery == False:
    result = '|'.join(str(x) for x in list(rec.values()))
    print(result)
  elif is_aggreagate == False:
    rec = dict((k, rec[k]) for k in select_subquery_list if k in rec)
    result = '|'.join(str(x) for x in list(rec.values()))
    print(result)
  else:
    AGGREGATION(rec)

def SELECTION(rec, where_condition):
  if eval(where_condition):
    return True
  else:
    return False

def JOIN(tables, join_statement):
  record=''
  for i in range(len(tables)):
    record+=table_line[i].rstrip()+'|'
  record = record.split('|')
  record.pop()
  record = convert_to_type(record)

  if(len(record) < len(column_name_list)) and not is_join:
    rec = dict(zip(join_column_list, record))
  else:
    rec = dict(zip(column_name_list, record))
    if is_join and not eval(join_statement):
      return ''
  return rec


def READ(tables, where_condition, join_statement, counter=0):
  global limit;
  with open(dataset_folder_path + tables[counter] + '.dat', 'r') as table_dict[counter]:
    for table_line[counter] in table_dict[counter]:
      if counter < len(tables)-1:
        end = READ(tables, where_condition, join_statement, counter+1)
        if end == True:
          return True
      else:
        rec = JOIN(tables, join_statement)
        if rec == '':
          continue
        selection = SELECTION(rec, where_condition)
        if selection == True:
          end = LIMIT(rec)
          if end == True:
            return True


def main(dataset_folder_path, sql_file_path):

  global table_dict;
  global table_line;
  global select_function;
  global final_list; 
  global is_aggreagate;
  global is_subquery;
  global is_join;
  global join_column_list;
  global select_columns_list;
  global select_subquery_list;
  global limit;

  read_content()
  table_dict = {}
  table_line = {}

  for i in range(len(select_dict)):

    column_header = select_dict[i].column_header
    print('\n' + '|'.join(column_header))
    where_statement = select_dict[i].where_statement
    table_names = select_dict[i].table_names
    select_columns = select_dict[i].columns
    parsed = select_dict[i].parsed()
    select_columns_list = select_dict[i].select_columns
    is_aggreagate = select_dict[i].is_aggregate
    is_subquery = select_dict[i].is_subquery
    is_join = select_dict[i].is_join
    join_column_list = select_dict[i].join_column_list
    limit = select_dict[i].limit
    select_subquery_list = select_dict[i].select_subquery
    select_function = select_dict[i].select_function
    join_statement = select_dict[i].join_statement
    final_list = {i: [] for i in range(len(select_function))}
    READ(table_names, where_statement, join_statement)
    print_aggregate_result(select_function, final_list)
    print('')

try:
  dataset_folder_path = sys.argv[1]
  sql_file_path = sys.argv[2]
  custom_select_query = ''
  if len(sys.argv)==4:
    custom_select_query = sys.argv[3]
  main(dataset_folder_path, sql_file_path)
except Exception as e:
  print('Exception occured:')
  print(e)
