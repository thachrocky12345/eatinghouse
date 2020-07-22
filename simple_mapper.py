from decimal import Decimal
import psycopg2
import logging
from querybuilder.query import Query
from django.db.models import Q
from django.db.models import QuerySet
from sbt_common import SbtCommon
import sqlalchemy

"""
  The Django querybuilder did not implement builders for insert, update and delete.
  this class extends querybuilder.query.Query and implements the methods.
"""
class SbtQuery (Query):
  """
    Creates delete SQL
  """
  def create_delete_sql(self, table=None, pk_field = None, pk_value=None,
                        **kwargs):
    #TODO if pk_value instanceof() conversion
    if kwargs and False:
      print('')

    sql = 'DELETE FROM ' + table + ' WHERE ' + pk_field + ' = ' + "'" + \
          str(pk_value) + "'"
    self.sbt_sql = sql
    self.sbt_args = None

    return self

  """
    Creates update SQL
  """
  def create_update_sql(self, table=None, field_names=None, values=None,
                        pk_field = None, pk_value=None, **kwargs):
    if kwargs and False:
      print('')

    sql = 'UPDATE ' + table
    field_str = ''
    if field_names and len(field_names) > 0 :
      for field in field_names :
        if len(field_str) > 0 :
          field_str += ','
        field_str += field + '=' + '%s'

    sql += ' SET ' + field_str + ' WHERE ' + pk_field + " = '" + str(pk_value) + "';"

    self.sbt_sql = sql
    self.sbt_args = values

    return self

  """
    Creates insert SQL
  """
  def create_insert_sql(self, table=None, field_names=None, values=None,
                        **kwargs):
    if kwargs and False:
      print('')

    sql = 'INSERT INTO ' + table
    field_str = ''
    if field_names and len(field_names) > 0 :
      for field in field_names :
        if len(field_str) > 0 :
          field_str += ','
        field_str += field

    value_str = ''
    if values and len(values) > 0 :
      counter = 0
      while counter < len(values) :
        if len(value_str) > 0 :
          value_str += ','
        value_str += '%s'
        counter += 1

    sql += ' (' + field_str + ') VALUES (' + value_str + ')'

    self.sbt_sql = sql
    self.sbt_args = values

    return self

  """
    Overrides the default get_sql methods so it return sql for insert,
    update and delete.
  """
  def get_sql(self, debug=False, use_cache=True):
    if self.sbt_sql and len (self.sbt_sql) > 0:
      return self.sbt_sql
    else :
      return Query.get_sql(self, debug=debug, use_cache=use_cache)

  """
    Overrides the default get_args methods so it return args for insert,
    update and delete.
  """
  def get_args(self):
    if self.sbt_args and len (self.sbt_args) > 0:
      return self.sbt_args
    else :
      return Query.get_args(self)

"""
  Generic database accessor class for Postgres
"""
class PostgresAccessor:

  """
    Initializes the class
  """
  def __init__(self, config, log=None):
    self._sbtcommon = SbtCommon()
    self._logger = self._sbtcommon.get_global_logger() if not log else log
    self._host = config['host']
    self._port = config['port']
    self._credentials = config['credentials']
    self._user = config['user']
    self._dbname = config['database']
    self._connection_string = "dbname='" + self._dbname + "' user='" + \
                              self._user + "' host='" + self._host + \
                              "' password=" + self._credentials
    # dialect+driver://username:password@host:port/database
    self._engine = sqlalchemy.create_engine(
      'postgresql+psycopg2://{user}:{password}@{host}/{db}'.format(
              user=self._user, password=self._credentials,
              host=self._host, db=self._dbname
      )
    )

  """
    Creates DJango Query class for insert statements
  """
  def create_django_insert_builder (self, table_name, field_names, values) :
    return SbtQuery().create_insert_sql(table_name, field_names, values)

  """
    Creates DJango Query class for update statements
  """
  def create_django_update_builder (self, table_name, field_names, values,
                                    pk_field, pk_value) :
    return SbtQuery().create_update_sql(table_name, field_names, values,
                                        pk_field, pk_value)

  """
    Creates DJango Query class for delete statements
  """
  def create_django_delete_builder (self, table_name, pk_field, pk_value) :
    return SbtQuery().create_delete_sql(table_name, pk_field, pk_value)

  """
    Creates DJango Query class for select statements
  """
  def create_django_query_builder (self, table_name, return_columns=None,
                                   and_conditions=None, or_conditions=None,
                                   in_condition=None, order_by_condition=None):
    if return_columns and len(return_columns) > 0 :
      query = Query().from_table(table_name, return_columns)
    else :
      query = Query().from_table(table_name)

    if and_conditions and len(and_conditions) > 0:
      for cond in and_conditions:
        query.where(**cond)

    if or_conditions and len(or_conditions) > 0:
      for or_cond in or_conditions:
        for key, value in or_cond.items():
          # criteria
          q = Q(**{key: value})
          # include q into query statement
          query._where.wheres.children.extend(q.children)
          # add connector
          query._where.wheres.add(q, 'OR')

    if in_condition is not None:
      # in_condition should be a dict like {'key__in': [values]}
      # get the key
      key = list(in_condition.keys())[0]
      # get the values
      value = list(in_condition.values())[0]
      # create the criteria object
      q = (key, value)
      # append condition to wheres children list
      query._where.wheres.children.append(q)

    if order_by_condition and len(order_by_condition) > 0:
      query.order_by(order_by_condition,table_name,False)

    return query

  """
    Executes a DJango Query class for insert, update or delete
  """
  def _execute_django_dml (self, query, raise_exception=False):
      self._execute_dml(query.get_sql(self), query.get_args(), raise_exception)

  """
    Executes a DJango Query class for selects
  """
  def _execute_django_query (self, query):
      return self._execute_query(query.get_sql(), query.get_args())

  """
    Executes a an insert, update or delete sql statement
  """
  def _execute_dml (self, query, args, raise_exception=False):
    conn = None
    cur = None
    try :
      conn = psycopg2.connect(self._connection_string)
      cur  = conn.cursor()
      cur.execute(query, args)
      conn.commit()
    except Exception as e:
      if raise_exception :
        raise e
      else :
        logging.info('Error: Encountered exception ' + str(e))
    finally :
      if cur :
        cur.close()
      if conn :
        conn.close()

  """
    Executes a an insert, update or delete sql statement
  """
  def _execute_query (self, query, args):
    response = None
    conn = None
    cur = None
    try :
      logging.debug('Executing query: ' + query + '\n args: ' + str(args))
      conn = psycopg2.connect(self._connection_string)
      conn.autocommit = True
      cur = conn.cursor()
      cur.execute(query, args)
      column_names = [desc[0] for desc in cur.description]
      query_data = cur.fetchall()
      if query_data and len(query_data) > 0 :
        response = []
        for row in query_data :
          row_dict = {}
          counter = 0
          for column in column_names :
            value = row[counter]
            #Decimal type will not convert to JSON
            if isinstance(value, Decimal) :
              value = float(value)
            row_dict[column] = value
            counter += 1
          response.append(row_dict)
    except Exception as e:
      logging.info('Error: Encountered exception ' + str(e))
    finally :
      if cur :
        cur.close()
      if conn :
        conn.close()

    return response

  def _make_large_object(self, ldata):
    """
    Creates a large object storing the provided data
    :param ldata: The data to be stored in the blob
    :return: The large object id created or None if error
    """
    loid = None

    try:
      conn = psycopg2.connect(self._connection_string)
      lobj = conn.lobject(0, 'w', 0)
      data = str.encode(ldata)
      len_value  = lobj.write(data)

      logging.debug('largeobject bytes written: ' + str(len_value))

      conn.commit()
      conn.close()

      loid = lobj.oid
    except Exception as e:
      logging.error('Error: Encountered exception ' + str(e))

    return loid

  def _del_large_object(self, loid):
    """
    Removes the specified large object from the DB
    :param loid: The id of the large object to be removed
    :return: True if success, False otherwise
    """
    rval = True

    try:
      conn = psycopg2.connect(self._connection_string)
      lobj = conn.lobject(loid, 'r', 0)

      lobj.unlink()
      logging.debug('largeobject ' + str(loid) + ' removed')

      conn.commit()
      conn.close()
    except Exception as e:
      logging.error('Encountered exception: ' + str(e) +
                    '\nRetrieving blob id: ' + str(loid))
      rval = False

    return rval

  def _retrieve_large_object(self, loid):
    """
    Retrieves the data stored in the specified large object
    :param loid: The id of the large object to be retrieved
    :return: The data stored i the large object None if error
    """
    data = None

    try:
      conn = psycopg2.connect(self._connection_string)
      lobj = conn.lobject(loid, 'r', 0)

      lobj.seek(0)

      data = lobj.read()

      logging.debug('largeobject bytes read: ' +  str(len(data)))

      conn.commit()
      conn.close()
    except Exception as e:
      logging.error('Error: Encountered exception retrieving object id ' +
                    str(e))

    return data

  def _get_db_tables(self):
    """
    Retrieve a tuple list of all tables in current DB (tuple [0] = db name)
    :return: res is the result tuple list
    """
    conn = psycopg2.connect(self._connection_string)
    cursor = conn.cursor()
    cursor.execute("select relname from pg_class where (relkind='r' or relkind='v') and relname !~ '^(pg_|sql_)' \
                    and pg_catalog.pg_table_is_visible(oid)")
    res = cursor.fetchall()

    return res

  def _get_table_fields(self, table):
    """
    Retrieve a list of all fields for the specified db table
    :param table:
    :return: fields is the list of all table fields
    """
    fields = []
    conn   = psycopg2.connect(self._connection_string)
    cursor = conn.cursor()
    try:
      cursor.execute('select * from ' + table + ' limit 1')
      fields = [desc[0] for desc in cursor.description]
    except Exception as e:
      print(str(e))

    return fields
