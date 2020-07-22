from pg_accessor import PostgresAccessor


class PostgresAccessorBulk(PostgresAccessor):
    _table_name = ''
    _columns = []

    def delete(self, data,  where_columns=["guid"]):
        args, column_statements = self.generate_condition_statment_and_args(data, where_columns)
        sql = """
        DELETE FROM {table} 
        WHERE {column_statements}
        """.format(table=self._table_name,
                   column_statements=column_statements)
        self._execute_dml(sql, args, raise_exception=True)

    def select(self, data, where_columns=["guid"]):
        res = self.select_all(data, where_columns)
        if res:
            return res[0]

    def select_all(self, data, where_columns=["guid"]):
        args, column_statements = self.generate_condition_statment_and_args(data, where_columns)
        sql = """
        SELECT * FROM {table}
        WHERE {column_statements}
        """.format(table=self._table_name,
                   column_statements=column_statements)
        res = self._execute_query(sql, args)
        if res:
            return res
        return []

    @staticmethod
    def generate_condition_statment_and_args(data, where_columns):
        column_statements = [" {} = %s ".format(col) for col in where_columns]
        column_statements = " AND ".join(column_statements)
        args = [data[col] for col in where_columns]
        return args, column_statements

    def insert(self, data):
        field_names = []
        field_args = []
        try:
            if data:
                for key, value in data.items():
                    if key == "guid" and value:
                        new_guid = value
                    field_names.append(str(key).lower())
                    field_args.append(value)

                for f in self._columns:
                    if f not in field_names:
                        if f == 'guid':
                            new_guid = self._sbtcommon.get_uuid()
                            field_args.append(str(new_guid))
                            field_names.append(f)
                        if f in ('created', 'create_time'):
                            field_names.append(f)
                            field_args.append(self._sbtcommon.DateUtil.get_current_iso_timestamp())

                query = self.create_django_insert_builder(self._table_name, field_names, field_args)
                query.sbt_sql = query.sbt_sql + " ON CONFLICT DO NOTHING RETURNING *"
                res = self._execute_query(query.sbt_sql, query.sbt_args)
                if res:
                    return res[0]
            else:
                raise ValueError('Data is missing for query')
        except Exception as e:
            self._logger.error('Adding portfolio to portfolio manager encountered exception: ' + str(e))

    def update(self, data, pk_field, pk_value):
        field_names = []
        field_args = []
        try:
            if data:
                for key, value in data.items():
                    field_names.append(str(key).lower())
                    field_args.append(value)

                for f in self._columns:
                    if f not in field_names:
                        if f == "modified":
                            field_names.append(f)
                            field_args.append(self._sbtcommon.DateUtil.get_current_iso_timestamp())

                query = self.create_django_update_builder(self._table_name, field_names,
                                                          field_args, pk_field, pk_value)
                query.sbt_sql = query.sbt_sql.replace(";", "") + " RETURNING *"
                res = self._execute_query(query.sbt_sql, query.sbt_args)
                if res:
                    return res[0]

            else:
                raise ValueError('Data is missing for query in tp_accessor - create_managed_pf() ')

        except Exception as e:
            self._logger.error('Adding portfolio to portfolio manager encountered exception: ' + str(e))

    def bulk_insert(self, data, block=3000):
        if not isinstance(data, list):
            raise ValueError("bulks data must be list type")

        divider = len(data) // block
        start = 0
        for index in range(0, divider + 1):
            end = start + block
            if end >= len(data):
                end = len(data)
            if start == end:
                continue
            self.bulk_insert_block_data(data[start:end])
            start = end

    def bulk_insert_block_data(self, block_data):
        insert_sql, args_values = self.generate_bulk_insert_sql_and_args_value(block_data)
        self._execute_dml(insert_sql, args_values, raise_exception=True)

    def generate_bulk_insert_sql_and_args_value(self, data):
        insert_columns = [col for col in self._columns if col in data[0].keys()]
        insert_columns_statement = ",".join(insert_columns)
        insert_columns_statement = "INSERT INTO {table} ({columns}) VALUES ".format(table=self._table_name,
                                                                                   columns=insert_columns_statement)
        insert_values_statement = ",".join(['%s']*len(insert_columns))
        insert_values_statement = "({})".format(insert_values_statement)
        insert_values_statement = ",".join([insert_values_statement] * len(data))

        insert_sql = insert_columns_statement + insert_values_statement
        args_values = []
        for dictionay_row in data:
            args_values += [dictionay_row[col] for col in insert_columns]

        return insert_sql, args_values



