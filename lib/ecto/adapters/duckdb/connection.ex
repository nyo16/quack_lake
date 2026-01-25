defmodule Ecto.Adapters.DuckDB.Connection do
  @moduledoc """
  SQL query and DDL generation for DuckDB.

  This module implements `Ecto.Adapters.SQL.Connection` for DuckDB,
  generating SQL queries compatible with DuckDB's dialect.
  """

  @behaviour Ecto.Adapters.SQL.Connection

  alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr, WithExpr}
  alias QuackLake.DBConnection.Query

  @parent_as __MODULE__

  # Query API

  @impl true
  def all(query, as_prefix \\ []) do
    sources = create_names(query, as_prefix)
    {select, order_by_distinct, select_distinct} = select(query, sources)

    from = from(query, sources)
    join = join(query, sources)
    where = where(query, sources)
    group_by = group_by(query, sources)
    having = having(query, sources)
    window = window(query, sources)
    combinations = combinations(query)
    order_by = order_by(query, order_by_distinct, sources)
    limit = limit(query, sources)
    offset = offset(query, sources)
    lock = lock(query, sources)

    [
      cte(query, sources),
      select,
      select_distinct,
      from,
      join,
      where,
      group_by,
      having,
      window,
      combinations,
      order_by,
      limit,
      offset,
      lock
    ]
    |> IO.iodata_to_binary()
  end

  @impl true
  def update_all(query, prefix \\ nil) do
    %{from: %{source: source}} = query
    sources = create_names(query, [])
    {from, name} = get_source(query, sources, 0, source)

    prefix = prefix || prefix(query)
    table = [prefix, from]

    fields = update_fields(query, sources)
    where = where(query, sources)

    ["UPDATE ", table, " AS ", name, " SET ", fields, where]
    |> IO.iodata_to_binary()
  end

  @impl true
  def delete_all(query) do
    %{from: %{source: source}} = query
    sources = create_names(query, [])
    {from, name} = get_source(query, sources, 0, source)

    where = where(query, sources)

    ["DELETE FROM ", from, " AS ", name, where]
    |> IO.iodata_to_binary()
  end

  @impl true
  def insert(prefix, table, header, rows, on_conflict, returning, placeholders) do
    counter_offset = length(placeholders)

    values =
      if header == [] do
        [" DEFAULT VALUES"]
      else
        [
          " (",
          intersperse_map(header, ?,, &quote_name/1),
          ") ",
          insert_all(rows, counter_offset)
        ]
      end

    [
      "INSERT INTO ",
      quote_table(prefix, table),
      values,
      on_conflict(on_conflict, header),
      returning(returning)
    ]
    |> IO.iodata_to_binary()
  end

  defp insert_all(rows, counter) do
    [
      "VALUES ",
      intersperse_reduce(rows, ?,, counter, fn row, counter ->
        {[?(, intersperse_map_reduce(row, ?,, counter, &insert_each/2), ?)], counter}
      end)
      |> elem(0)
    ]
  end

  defp insert_each({:placeholder, idx}, counter), do: {["$", Integer.to_string(idx)], counter}

  defp insert_each(nil, counter), do: {["$", Integer.to_string(counter + 1)], counter + 1}

  defp insert_each(_, counter) do
    {["$", Integer.to_string(counter + 1)], counter + 1}
  end

  defp on_conflict({:raise, [], []}, _header), do: []

  defp on_conflict({:nothing, [], targets}, _header) do
    conflict_target(targets) ++ [" DO NOTHING"]
  end

  defp on_conflict({:replace_all, [], {:constraint, _} = target}, header) do
    conflict_target(target) ++
      [" DO UPDATE SET ", intersperse_map(header, ?,, &conflict_field/1)]
  end

  defp on_conflict({:replace_all, [], targets}, header) do
    conflict_target(targets) ++
      [" DO UPDATE SET ", intersperse_map(header, ?,, &conflict_field/1)]
  end

  defp on_conflict({fields, [], targets}, _header) when is_list(fields) do
    conflict_target(targets) ++
      [" DO UPDATE SET ", intersperse_map(fields, ?,, &conflict_field/1)]
  end

  defp on_conflict({query, [], targets}, _header) do
    conflict_target(targets) ++
      [" DO UPDATE SET " | update_fields(query, create_names(query, []))]
  end

  defp conflict_target({:constraint, name}), do: [" ON CONFLICT ON CONSTRAINT ", quote_name(name)]
  defp conflict_target({:unsafe_fragment, fragment}), do: [" ON CONFLICT ", fragment]
  defp conflict_target([]), do: [" ON CONFLICT"]

  defp conflict_target(targets) do
    [" ON CONFLICT (", intersperse_map(targets, ?,, &quote_name/1), ")"]
  end

  defp conflict_field(field), do: [quote_name(field), " = EXCLUDED.", quote_name(field)]

  @impl true
  def update(prefix, table, fields, filters, returning) do
    [
      "UPDATE ",
      quote_table(prefix, table),
      " SET ",
      intersperse_map(fields, ?,, fn {field, _} ->
        [quote_name(field), " = $", Integer.to_string(field_index(field, fields) + 1)]
      end),
      " WHERE ",
      intersperse_map(filters, " AND ", fn {field, _} ->
        [
          quote_name(field),
          " = $",
          Integer.to_string(field_index(field, fields) + length(fields) + 1)
        ]
      end),
      returning(returning)
    ]
    |> IO.iodata_to_binary()
  end

  defp field_index(field, fields) do
    Enum.find_index(fields, fn {f, _} -> f == field end)
  end

  @impl true
  def delete(prefix, table, filters, returning) do
    [
      "DELETE FROM ",
      quote_table(prefix, table),
      " WHERE ",
      intersperse_map(filters, " AND ", fn {field, idx} ->
        [quote_name(field), " = $", Integer.to_string(idx)]
      end),
      returning(returning)
    ]
    |> IO.iodata_to_binary()
  end

  @impl true
  def explain_query(conn, query, params, opts) do
    type = Keyword.get(opts, :type, :physical)

    explain_type =
      case type do
        :physical -> "EXPLAIN "
        :analyze -> "EXPLAIN ANALYZE "
        :logical -> "EXPLAIN (LOGICAL) "
      end

    DBConnection.execute(
      conn,
      %QuackLake.DBConnection.Query{statement: explain_type <> query},
      params,
      opts
    )
  end

  # DDL

  @impl true
  def execute_ddl({command, %Ecto.Migration.Table{} = table, columns})
      when command in [:create, :create_if_not_exists] do
    query = [
      if_do(command == :create_if_not_exists, "CREATE TABLE IF NOT EXISTS ", "CREATE TABLE "),
      quote_table(table.prefix, table.name),
      ?\s,
      ?(,
      column_definitions(table, columns),
      pk_definition(columns),
      ?),
      options_expr(table.options)
    ]

    [query]
  end

  def execute_ddl({command, %Ecto.Migration.Table{} = table, _columns})
      when command in [:drop, :drop_if_exists] do
    [
      [
        if_do(command == :drop_if_exists, "DROP TABLE IF EXISTS ", "DROP TABLE "),
        quote_table(table.prefix, table.name)
      ]
    ]
  end

  def execute_ddl({:alter, %Ecto.Migration.Table{} = table, changes}) do
    Enum.map(changes, fn change ->
      [
        "ALTER TABLE ",
        quote_table(table.prefix, table.name),
        ?\s,
        column_change(table, change)
      ]
    end)
  end

  def execute_ddl({command, %Ecto.Migration.Index{} = index})
      when command in [:create, :create_if_not_exists] do
    fields = intersperse_map(index.columns, ", ", &index_expr/1)

    [
      [
        if_do(command == :create_if_not_exists, "CREATE INDEX IF NOT EXISTS ", "CREATE INDEX "),
        if_do(index.unique, "UNIQUE ", []),
        quote_name(index.name),
        " ON ",
        quote_table(index.prefix, index.table),
        " (",
        fields,
        ")"
      ]
    ]
  end

  def execute_ddl({command, %Ecto.Migration.Index{} = index, _mode})
      when command in [:drop, :drop_if_exists] do
    [
      [
        if_do(command == :drop_if_exists, "DROP INDEX IF EXISTS ", "DROP INDEX "),
        quote_name(index.name)
      ]
    ]
  end

  def execute_ddl(
        {:rename, %Ecto.Migration.Table{} = current_table, %Ecto.Migration.Table{} = new_table}
      ) do
    [
      [
        "ALTER TABLE ",
        quote_table(current_table.prefix, current_table.name),
        " RENAME TO ",
        quote_table(new_table.prefix, new_table.name)
      ]
    ]
  end

  def execute_ddl({:rename, %Ecto.Migration.Table{} = table, current_column, new_column}) do
    [
      [
        "ALTER TABLE ",
        quote_table(table.prefix, table.name),
        " RENAME COLUMN ",
        quote_name(current_column),
        " TO ",
        quote_name(new_column)
      ]
    ]
  end

  def execute_ddl(string) when is_binary(string), do: [string]

  def execute_ddl(keyword) when is_list(keyword) do
    raise ArgumentError, "DuckDB adapter does not support keyword DDL statements"
  end

  # Column definitions

  defp column_definitions(table, columns) do
    intersperse_map(columns, ", ", &column_definition(table, &1))
  end

  defp column_definition(table, {:add, name, %Ecto.Migration.Reference{} = ref, opts}) do
    [
      quote_name(name),
      ?\s,
      reference_column_type(ref.type, opts),
      column_options(name, opts),
      reference_constraint(table, name, ref)
    ]
  end

  defp column_definition(_table, {:add, name, type, opts}) do
    [quote_name(name), ?\s, column_type(type, opts), column_options(name, opts)]
  end

  defp column_change(table, {:add, name, %Ecto.Migration.Reference{} = ref, opts}) do
    [
      "ADD COLUMN ",
      quote_name(name),
      ?\s,
      reference_column_type(ref.type, opts),
      column_options(name, opts),
      reference_constraint(table, name, ref)
    ]
  end

  defp column_change(_table, {:add, name, type, opts}) do
    ["ADD COLUMN ", quote_name(name), ?\s, column_type(type, opts), column_options(name, opts)]
  end

  defp column_change(_table, {:modify, name, type, opts}) do
    ["ALTER COLUMN ", quote_name(name), " TYPE ", column_type(type, opts)]
  end

  defp column_change(_table, {:remove, name}) do
    ["DROP COLUMN ", quote_name(name)]
  end

  defp column_change(_table, {:remove, name, _type, _opts}) do
    ["DROP COLUMN ", quote_name(name)]
  end

  defp column_options(name, opts) do
    default = Keyword.get(opts, :default)
    null = Keyword.get(opts, :null)
    pk = Keyword.get(opts, :primary_key)

    [
      default_expr(default, name),
      null_expr(null),
      if_do(pk, " PRIMARY KEY")
    ]
  end

  defp null_expr(false), do: " NOT NULL"
  defp null_expr(true), do: " NULL"
  defp null_expr(_), do: []

  defp default_expr(nil, _name), do: []
  defp default_expr(literal, _name) when is_binary(literal), do: [" DEFAULT '", literal, "'"]
  defp default_expr(literal, _name) when is_number(literal), do: [" DEFAULT ", to_string(literal)]

  defp default_expr(literal, _name) when is_boolean(literal),
    do: [" DEFAULT ", to_string(literal)]

  defp default_expr({:fragment, expr}, _name), do: [" DEFAULT ", expr]

  defp pk_definition(columns) do
    pks =
      for {:add, name, _type, opts} <- columns,
          opts[:primary_key],
          do: name

    case pks do
      [] -> []
      _ -> [", PRIMARY KEY (", intersperse_map(pks, ", ", &quote_name/1), ")"]
    end
  end

  defp reference_column_type(:serial, _opts), do: "INTEGER"
  defp reference_column_type(:bigserial, _opts), do: "BIGINT"
  defp reference_column_type(type, opts), do: column_type(type, opts)

  defp reference_constraint(_table, _name, %{with: []}), do: []

  defp reference_constraint(table, name, ref) do
    [
      ", FOREIGN KEY (",
      quote_name(name),
      ") REFERENCES ",
      quote_table(ref.prefix || table.prefix, ref.table),
      "(",
      quote_name(ref.column),
      ")",
      reference_on_delete(ref.on_delete),
      reference_on_update(ref.on_update)
    ]
  end

  defp reference_on_delete(:nothing), do: " ON DELETE NO ACTION"
  defp reference_on_delete(:delete_all), do: " ON DELETE CASCADE"
  defp reference_on_delete(:nilify_all), do: " ON DELETE SET NULL"
  defp reference_on_delete(:restrict), do: " ON DELETE RESTRICT"
  defp reference_on_delete(_), do: []

  defp reference_on_update(:nothing), do: " ON UPDATE NO ACTION"
  defp reference_on_update(:update_all), do: " ON UPDATE CASCADE"
  defp reference_on_update(:nilify_all), do: " ON UPDATE SET NULL"
  defp reference_on_update(:restrict), do: " ON UPDATE RESTRICT"
  defp reference_on_update(_), do: []

  defp index_expr(literal) when is_binary(literal), do: literal
  defp index_expr(literal), do: quote_name(literal)

  # Type conversions

  defp column_type(:id, _opts), do: "BIGINT"
  defp column_type(:serial, _opts), do: "INTEGER"
  defp column_type(:bigserial, _opts), do: "BIGINT"
  defp column_type(:binary_id, _opts), do: "UUID"
  defp column_type(:uuid, _opts), do: "UUID"
  defp column_type(:string, opts), do: varchar_type(opts)
  defp column_type(:binary, _opts), do: "BLOB"
  defp column_type(:integer, _opts), do: "INTEGER"
  defp column_type(:bigint, _opts), do: "BIGINT"
  defp column_type(:float, _opts), do: "DOUBLE"
  defp column_type(:decimal, opts), do: decimal_type(opts)
  defp column_type(:boolean, _opts), do: "BOOLEAN"
  defp column_type(:map, _opts), do: "JSON"
  defp column_type({:map, _}, _opts), do: "JSON"
  defp column_type(:date, _opts), do: "DATE"
  defp column_type(:time, _opts), do: "TIME"
  defp column_type(:time_usec, _opts), do: "TIME"
  defp column_type(:naive_datetime, _opts), do: "TIMESTAMP"
  defp column_type(:naive_datetime_usec, _opts), do: "TIMESTAMP"
  defp column_type(:utc_datetime, _opts), do: "TIMESTAMPTZ"
  defp column_type(:utc_datetime_usec, _opts), do: "TIMESTAMPTZ"
  defp column_type(:timestamp, _opts), do: "TIMESTAMP"
  defp column_type({:array, type}, opts), do: [column_type(type, opts), "[]"]
  defp column_type(other, _opts) when is_atom(other), do: Atom.to_string(other) |> String.upcase()

  defp varchar_type(opts) do
    case Keyword.get(opts, :size) do
      nil -> "VARCHAR"
      size -> ["VARCHAR(", Integer.to_string(size), ")"]
    end
  end

  defp decimal_type(opts) do
    precision = Keyword.get(opts, :precision)
    scale = Keyword.get(opts, :scale, 0)

    case {precision, scale} do
      {nil, _} -> "DECIMAL"
      {p, s} -> ["DECIMAL(", Integer.to_string(p), ",", Integer.to_string(s), ")"]
    end
  end

  defp options_expr(nil), do: []
  defp options_expr(options), do: [?\s, options]

  # Query generation helpers

  defp cte(%{with_ctes: %WithExpr{recursive: recursive, queries: [_ | _] = queries}}, _sources) do
    recursive_opt = if recursive, do: "RECURSIVE ", else: ""

    ctes =
      intersperse_map(queries, ", ", fn %{name: name} = query ->
        cte_query =
          case query do
            %{query: query} -> all(query)
            %{literal: literal} -> literal
          end

        [quote_name(name), " AS (", cte_query, ")"]
      end)

    ["WITH ", recursive_opt, ctes, " "]
  end

  defp cte(_, _), do: []

  defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources) do
    select_distinct = select_distinct(distinct, sources, query)
    order_by_distinct = order_by_distinct(distinct, sources, query)
    {["SELECT " | select_fields(fields, sources, query)], order_by_distinct, select_distinct}
  end

  defp select_distinct(nil, _sources, _query), do: []
  defp select_distinct(%QueryExpr{expr: true}, _sources, _query), do: " DISTINCT"
  defp select_distinct(%QueryExpr{expr: false}, _sources, _query), do: []

  defp select_distinct(%QueryExpr{expr: exprs}, sources, query) when is_list(exprs) do
    [" DISTINCT ON (", intersperse_map(exprs, ", ", &expr(&1, sources, query)), ")"]
  end

  defp order_by_distinct(nil, _sources, _query), do: []
  defp order_by_distinct(%QueryExpr{expr: true}, _sources, _query), do: []
  defp order_by_distinct(%QueryExpr{expr: false}, _sources, _query), do: []

  defp order_by_distinct(%QueryExpr{expr: exprs}, _sources, _query) when is_list(exprs) do
    Enum.map(exprs, &{:asc, &1})
  end

  defp select_fields([], _sources, _query), do: "'TRUE'"

  defp select_fields(fields, sources, query) do
    intersperse_map(fields, ", ", fn
      {:&, _, [idx, fields, _counter]} ->
        case elem(sources, idx) do
          {source, _, nil} ->
            error!(
              query,
              "DuckDB does not support selecting all fields from #{source} without a schema. " <>
                "Please specify a schema or use a fragment."
            )

          {_, source, _} ->
            select_fields_from_schema(fields, source)
        end

      {key, value} ->
        [expr(value, sources, query), " AS ", quote_name(key)]

      value ->
        expr(value, sources, query)
    end)
  end

  defp select_fields_from_schema(fields, source) do
    intersperse_map(fields, ", ", fn field ->
      [source, ?. | quote_name(field)]
    end)
  end

  defp from(%{from: nil}, _sources), do: []

  defp from(%{from: %{source: source, hints: hints}} = query, sources) do
    {from, name} = get_source(query, sources, 0, source)
    [" FROM ", from, " AS ", name, hints(hints)]
  end

  defp join(%{joins: []}, _sources), do: []

  defp join(%{joins: joins} = query, sources) do
    Enum.map(joins, fn
      %JoinExpr{qual: qual, ix: ix, source: source, on: %QueryExpr{expr: on_expr}, hints: hints} ->
        {join, name} = get_source(query, sources, ix, source)

        [
          join_qual(qual),
          join,
          " AS ",
          name,
          hints(hints),
          join_on(qual, on_expr, sources, query)
        ]
    end)
  end

  defp join_on(:cross, true, _sources, _query), do: []
  defp join_on(:cross_lateral, true, _sources, _query), do: []

  defp join_on(_qual, on_expr, sources, query) do
    [" ON ", expr(on_expr, sources, query)]
  end

  defp join_qual(:inner), do: " INNER JOIN "
  defp join_qual(:inner_lateral), do: " INNER JOIN LATERAL "
  defp join_qual(:left), do: " LEFT OUTER JOIN "
  defp join_qual(:left_lateral), do: " LEFT OUTER JOIN LATERAL "
  defp join_qual(:right), do: " RIGHT OUTER JOIN "
  defp join_qual(:full), do: " FULL OUTER JOIN "
  defp join_qual(:cross), do: " CROSS JOIN "
  defp join_qual(:cross_lateral), do: " CROSS JOIN LATERAL "

  defp where(%{wheres: []}, _sources), do: []

  defp where(%{wheres: wheres} = query, sources) do
    [
      " WHERE "
      | intersperse_map(wheres, " AND ", fn %BooleanExpr{expr: expr} ->
          ["(", expr(expr, sources, query), ")"]
        end)
    ]
  end

  defp group_by(%{group_bys: []}, _sources), do: []

  defp group_by(%{group_bys: group_bys} = query, sources) do
    [
      " GROUP BY "
      | intersperse_map(group_bys, ", ", fn %QueryExpr{expr: expr} ->
          intersperse_map(expr, ", ", &expr(&1, sources, query))
        end)
    ]
  end

  defp having(%{havings: []}, _sources), do: []

  defp having(%{havings: havings} = query, sources) do
    [
      " HAVING "
      | intersperse_map(havings, " AND ", fn %BooleanExpr{expr: expr} ->
          ["(", expr(expr, sources, query), ")"]
        end)
    ]
  end

  defp window(%{windows: []}, _sources), do: []

  defp window(%{windows: windows} = query, sources) do
    [
      " WINDOW "
      | intersperse_map(windows, ", ", fn {name, %{expr: kw}} ->
          [quote_name(name), " AS ", window_exprs(kw, sources, query)]
        end)
    ]
  end

  defp window_exprs(kw, sources, query) do
    [?(, intersperse_map(kw, ?\s, &window_expr(&1, sources, query)), ?)]
  end

  defp window_expr({:partition_by, fields}, sources, query) do
    ["PARTITION BY " | intersperse_map(fields, ", ", &expr(&1, sources, query))]
  end

  defp window_expr({:order_by, fields}, sources, query) do
    ["ORDER BY " | intersperse_map(fields, ", ", &order_by_expr(&1, sources, query))]
  end

  defp window_expr({:frame, {:fragment, _, _} = fragment}, sources, query) do
    expr(fragment, sources, query)
  end

  defp combinations(%{combinations: []}), do: []

  defp combinations(%{combinations: combinations}) do
    Enum.map(combinations, fn
      {:union, query} -> [" UNION ", all(query)]
      {:union_all, query} -> [" UNION ALL ", all(query)]
      {:except, query} -> [" EXCEPT ", all(query)]
      {:except_all, query} -> [" EXCEPT ALL ", all(query)]
      {:intersect, query} -> [" INTERSECT ", all(query)]
      {:intersect_all, query} -> [" INTERSECT ALL ", all(query)]
    end)
  end

  defp order_by(%{order_bys: []}, _distinct, _sources), do: []

  defp order_by(%{order_bys: order_bys} = query, distinct, sources) do
    order_bys = Enum.flat_map(order_bys, & &1.expr)
    order_bys = distinct ++ order_bys

    [
      " ORDER BY "
      | intersperse_map(order_bys, ", ", &order_by_expr(&1, sources, query))
    ]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    [expr(expr, sources, query) | dir_str(dir)]
  end

  defp dir_str(:asc), do: " ASC"
  defp dir_str(:asc_nulls_first), do: " ASC NULLS FIRST"
  defp dir_str(:asc_nulls_last), do: " ASC NULLS LAST"
  defp dir_str(:desc), do: " DESC"
  defp dir_str(:desc_nulls_first), do: " DESC NULLS FIRST"
  defp dir_str(:desc_nulls_last), do: " DESC NULLS LAST"

  defp limit(%{limit: nil}, _sources), do: []

  defp limit(%{limit: %QueryExpr{expr: expr}} = query, sources) do
    [" LIMIT ", expr(expr, sources, query)]
  end

  defp offset(%{offset: nil}, _sources), do: []

  defp offset(%{offset: %QueryExpr{expr: expr}} = query, sources) do
    [" OFFSET ", expr(expr, sources, query)]
  end

  defp lock(%{lock: nil}, _sources), do: []
  # DuckDB doesn't support FOR UPDATE
  defp lock(%{lock: _}, _sources), do: []

  defp hints([]), do: []
  defp hints(hints), do: [" " | intersperse_map(hints, " ", &hint/1)]

  defp hint(hint) when is_binary(hint), do: hint

  defp returning([]), do: []

  defp returning(returning) do
    [" RETURNING " | intersperse_map(returning, ", ", &quote_name/1)]
  end

  defp update_fields(%{updates: updates} = query, sources) do
    for(
      %{expr: expr} <- updates,
      {op, kw} <- expr,
      {key, value} <- kw,
      do: update_op(op, key, value, sources, query)
    )
    |> Enum.intersperse(", ")
  end

  defp update_op(:set, key, value, sources, query) do
    [quote_name(key), " = ", expr(value, sources, query)]
  end

  defp update_op(:inc, key, value, sources, query) do
    [
      quote_name(key),
      " = ",
      quote_qualified_name(key, sources, query),
      " + ",
      expr(value, sources, query)
    ]
  end

  defp update_op(:push, key, value, sources, query) do
    [
      quote_name(key),
      " = array_append(",
      quote_qualified_name(key, sources, query),
      ", ",
      expr(value, sources, query),
      ")"
    ]
  end

  defp update_op(:pull, key, value, sources, query) do
    [
      quote_name(key),
      " = array_remove(",
      quote_qualified_name(key, sources, query),
      ", ",
      expr(value, sources, query),
      ")"
    ]
  end

  defp update_op(command, _key, _value, _sources, query) do
    error!(query, "Unknown update operation #{inspect(command)}")
  end

  defp quote_qualified_name(key, sources, _query) do
    {_, name, _} = elem(sources, 0)
    [name, ?. | quote_name(key)]
  end

  # Expression generation

  defp expr({:^, [], [idx]}, _sources, _query) do
    ["$", Integer.to_string(idx + 1)]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
    {_, name, _} = elem(sources, idx)
    [name, ?. | quote_name(field)]
  end

  defp expr({{:., _, [{:parent_as, _, [as]}, field]}, _, []}, _sources, _query)
       when is_atom(field) do
    [quote_name(as), ?. | quote_name(field)]
  end

  defp expr({:&, _, [idx]}, sources, _query) do
    {_, source, _} = elem(sources, idx)
    source
  end

  defp expr({:in, _, [_left, []]}, _sources, _query) do
    "FALSE"
  end

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [left, {:^, _, [idx, length]}]}, sources, query) do
    args =
      1..length
      |> Enum.map(&["$", Integer.to_string(idx + &1)])
      |> Enum.intersperse(?,)

    [expr(left, sources, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [left, %Ecto.SubQuery{} = subquery]}, sources, query) do
    [expr(left, sources, query), " IN ", expr(subquery, sources, query)]
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " IS NULL"]
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "DuckDB adapter does not support keyword or interpolated fragments")
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({:literal, _, [literal]}, _sources, _query) do
    quote_literal(literal)
  end

  defp expr({:selected_as, _, [name]}, _sources, _query) do
    quote_name(name)
  end

  defp expr({:datetime_add, _, [datetime, count, interval]}, sources, query) do
    [
      expr(datetime, sources, query),
      " + ",
      interval(count, interval, sources, query)
    ]
  end

  defp expr({:date_add, _, [date, count, interval]}, sources, query) do
    [
      expr(date, sources, query),
      " + ",
      interval(count, interval, sources, query)
    ]
  end

  defp expr({:filter, _, [agg, filter]}, sources, query) do
    [expr(agg, sources, query), " FILTER (WHERE ", expr(filter, sources, query), ?)]
  end

  defp expr({:over, _, [agg, name]}, sources, query) when is_atom(name) do
    [expr(agg, sources, query), " OVER " | quote_name(name)]
  end

  defp expr({:over, _, [agg, kw]}, sources, query) do
    [expr(agg, sources, query), " OVER " | window_exprs(kw, sources, query)]
  end

  defp expr(%Ecto.SubQuery{query: query}, sources, _query) do
    query = put_in(query.aliases[@parent_as], {nil, sources, nil})
    [?(, all(query), ?)]
  end

  defp expr({:exists, _, [subquery]}, sources, query) do
    ["EXISTS ", expr(subquery, sources, query)]
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    {modifier, args} =
      case args do
        [rest, {:distinct, true}] -> {" DISTINCT ", [rest]}
        _ -> {[], args}
      end

    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [?(, expr(left, sources, query), op, expr(right, sources, query), ?)]

      {:fun, fun} ->
        [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr(list, sources, query) when is_list(list) do
    ["ARRAY[", intersperse_map(list, ?,, &expr(&1, sources, query)), ?]]
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Ecto.Query.Tagged{value: binary, type: :binary}, _sources, _query)
       when is_binary(binary) do
    ["'\\x", Base.encode16(binary, case: :lower), "'::BLOB"]
  end

  defp expr(%Ecto.Query.Tagged{value: other, type: type}, sources, query) do
    [expr(other, sources, query), "::", ecto_to_db(type)]
  end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr(true, _sources, _query), do: "TRUE"
  defp expr(false, _sources, _query), do: "FALSE"
  defp expr(binary, _sources, _query) when is_binary(binary), do: [?', escape_string(binary), ?']
  defp expr(integer, _sources, _query) when is_integer(integer), do: Integer.to_string(integer)
  defp expr(float, _sources, _query) when is_float(float), do: Float.to_string(float)

  defp interval(count, interval, sources, query) do
    [
      "INTERVAL ",
      expr(count, sources, query),
      ?\s,
      interval_unit(interval)
    ]
  end

  defp interval_unit("year"), do: "YEAR"
  defp interval_unit("month"), do: "MONTH"
  defp interval_unit("week"), do: "WEEK"
  defp interval_unit("day"), do: "DAY"
  defp interval_unit("hour"), do: "HOUR"
  defp interval_unit("minute"), do: "MINUTE"
  defp interval_unit("second"), do: "SECOND"
  defp interval_unit("millisecond"), do: "MILLISECOND"
  defp interval_unit("microsecond"), do: "MICROSECOND"

  defp handle_call(fun, _arity) do
    case fun do
      :== -> {:binary_op, " = "}
      :!= -> {:binary_op, " != "}
      :<= -> {:binary_op, " <= "}
      :>= -> {:binary_op, " >= "}
      :< -> {:binary_op, " < "}
      :> -> {:binary_op, " > "}
      :+ -> {:binary_op, " + "}
      :- -> {:binary_op, " - "}
      :* -> {:binary_op, " * "}
      :/ -> {:binary_op, " / "}
      :and -> {:binary_op, " AND "}
      :or -> {:binary_op, " OR "}
      :like -> {:binary_op, " LIKE "}
      :ilike -> {:binary_op, " ILIKE "}
      fun -> {:fun, Atom.to_string(fun) |> String.upcase()}
    end
  end

  defp ecto_to_db(:id), do: "BIGINT"
  defp ecto_to_db(:serial), do: "INTEGER"
  defp ecto_to_db(:bigserial), do: "BIGINT"
  defp ecto_to_db(:binary_id), do: "UUID"
  defp ecto_to_db(:uuid), do: "UUID"
  defp ecto_to_db(:string), do: "VARCHAR"
  defp ecto_to_db(:binary), do: "BLOB"
  defp ecto_to_db(:integer), do: "INTEGER"
  defp ecto_to_db(:bigint), do: "BIGINT"
  defp ecto_to_db(:float), do: "DOUBLE"
  defp ecto_to_db(:decimal), do: "DECIMAL"
  defp ecto_to_db(:boolean), do: "BOOLEAN"
  defp ecto_to_db(:date), do: "DATE"
  defp ecto_to_db(:time), do: "TIME"
  defp ecto_to_db(:time_usec), do: "TIME"
  defp ecto_to_db(:naive_datetime), do: "TIMESTAMP"
  defp ecto_to_db(:naive_datetime_usec), do: "TIMESTAMP"
  defp ecto_to_db(:utc_datetime), do: "TIMESTAMPTZ"
  defp ecto_to_db(:utc_datetime_usec), do: "TIMESTAMPTZ"
  defp ecto_to_db(:map), do: "JSON"
  defp ecto_to_db({:map, _}), do: "JSON"
  defp ecto_to_db({:array, type}), do: [ecto_to_db(type), "[]"]
  defp ecto_to_db(other), do: Atom.to_string(other)

  # Helper functions

  defp create_names(%{sources: sources}, as_prefix) do
    create_names(sources, 0, tuple_size(sources), as_prefix) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit, as_prefix) when pos < limit do
    current = create_name(sources, pos, as_prefix)
    [current | create_names(sources, pos + 1, limit, as_prefix)]
  end

  defp create_names(_sources, pos, pos, _as_prefix) do
    []
  end

  defp create_name(sources, pos, as_prefix) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %Ecto.SubQuery{} ->
        {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
    end
  end

  defp create_alias(<<first, _rest::binary>>)
       when first in ?a..?z
       when first in ?A..?Z do
    first
  end

  defp create_alias(_), do: ?t

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} =
      elem(sources, ix) ||
        error!(query, "DuckDB adapter does not support multiple from sources")

    {expr || paren_expr(source, sources, query), name}
  end

  defp paren_expr(%Ecto.SubQuery{query: query}, sources, _query) do
    query = put_in(query.aliases[@parent_as], {nil, sources, nil})
    [?(, all(query), ?)]
  end

  defp paren_expr(%Ecto.Query.FromExpr{source: source}, sources, query) do
    paren_expr(source, sources, query)
  end

  defp paren_expr({:fragment, _, _} = expr, sources, query) do
    expr(expr, sources, query)
  end

  defp paren_expr({:values, _, _} = expr, sources, query) do
    [?(, expr(expr, sources, query), ?)]
  end

  defp prefix(%{prefix: nil}), do: []
  defp prefix(%{prefix: prefix}), do: [quote_name(prefix), ?.]

  defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))

  defp quote_name(name) do
    if String.contains?(name, "\"") do
      error!(nil, "bad literal/field/table name #{inspect(name)}")
    end

    [?", name, ?"]
  end

  defp quote_table(nil, name), do: quote_name(name)
  defp quote_table(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

  defp quote_literal(value) when is_binary(value) do
    [?', escape_string(value), ?']
  end

  defp quote_literal(value) when is_integer(value) do
    Integer.to_string(value)
  end

  defp quote_literal(value) when is_float(value) do
    Float.to_string(value)
  end

  defp intersperse_map(list, separator, mapper)

  defp intersperse_map([], _separator, _mapper), do: []
  defp intersperse_map([elem], _separator, mapper), do: [mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperse_map(rest, separator, mapper)]
  end

  defp intersperse_map_reduce(list, separator, user_acc, reducer)

  defp intersperse_map_reduce([], _separator, user_acc, _reducer), do: {[], user_acc}

  defp intersperse_map_reduce([elem], _separator, user_acc, reducer) do
    {result, user_acc} = reducer.(elem, user_acc)
    {[result], user_acc}
  end

  defp intersperse_map_reduce([elem | rest], separator, user_acc, reducer) do
    {result, user_acc} = reducer.(elem, user_acc)
    {rest_result, user_acc} = intersperse_map_reduce(rest, separator, user_acc, reducer)
    {[result, separator | rest_result], user_acc}
  end

  defp intersperse_reduce(list, separator, user_acc, reducer)

  defp intersperse_reduce([], _separator, user_acc, _reducer), do: {[], user_acc}

  defp intersperse_reduce([elem], _separator, user_acc, reducer) do
    reducer.(elem, user_acc)
  end

  defp intersperse_reduce([elem | rest], separator, user_acc, reducer) do
    {result, user_acc} = reducer.(elem, user_acc)
    {rest_result, user_acc} = intersperse_reduce(rest, separator, user_acc, reducer)
    {[result, separator | rest_result], user_acc}
  end

  defp escape_string(value) when is_binary(value) do
    :binary.replace(value, "'", "''", [:global])
  end

  defp if_do(condition, a, b \\ [])
  defp if_do(true, a, _b), do: a
  defp if_do(false, _a, b), do: b
  defp if_do(nil, _a, b), do: b

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  # Required DBConnection callbacks

  @impl true
  def child_spec(opts) do
    DBConnection.child_spec(QuackLake.DBConnection.Protocol, opts)
  end

  @impl true
  def query(conn, sql, params, opts) do
    query = %Query{statement: sql}
    DBConnection.execute(conn, query, params, opts)
  end

  @impl true
  def query_many(_conn, _sql, _params, _opts) do
    raise "DuckDB adapter does not support query_many"
  end

  @impl true
  def execute(conn, query, params, opts) do
    DBConnection.execute(conn, query, params, opts)
  end

  @impl true
  def prepare_execute(conn, _name, sql, params, opts) do
    query = %Query{statement: sql}

    case DBConnection.execute(conn, query, params, opts) do
      {:ok, result} -> {:ok, query, result}
      {:error, _} = err -> err
    end
  end

  @impl true
  def stream(_conn, _sql, _params, _opts) do
    raise "DuckDB adapter does not support streaming via DBConnection"
  end

  @impl true
  def ddl_logs(_result), do: []

  @impl true
  def table_exists_query(table) do
    {"SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1", [table]}
  end

  @impl true
  def to_constraints(_error, _opts), do: []
end
