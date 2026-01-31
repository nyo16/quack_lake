defmodule Ecto.Adapters.DuckLake.Connection do
  @moduledoc """
  SQL query generation for DuckLake.

  This module delegates to `Ecto.Adapters.DuckDB.Connection` since
  DuckLake uses the same SQL dialect as DuckDB.
  """

  # Delegate all functions to DuckDB.Connection
  @doc false
  defdelegate all(query), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate all(query, as_prefix), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate update_all(query), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate update_all(query, prefix), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate delete_all(query), to: Ecto.Adapters.DuckDB.Connection

  @doc false
  defdelegate insert(prefix, table, header, rows, on_conflict, returning, placeholders),
    to: Ecto.Adapters.DuckDB.Connection

  @doc false
  defdelegate update(prefix, table, fields, filters, returning),
    to: Ecto.Adapters.DuckDB.Connection

  @doc false
  defdelegate delete(prefix, table, filters, returning), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate execute_ddl(command), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate ddl_logs(result), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate table_exists_query(table), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate to_constraints(error, opts), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate explain_query(conn, query, params, opts), to: Ecto.Adapters.DuckDB.Connection

  # Use LakeProtocol for child_spec
  def child_spec(opts) do
    DBConnection.child_spec(QuackLake.DBConnection.LakeProtocol, opts)
  end

  @doc false
  defdelegate query(conn, sql, params, opts), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate query_many(conn, sql, params, opts), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate execute(conn, query, params, opts), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate prepare_execute(conn, name, sql, params, opts), to: Ecto.Adapters.DuckDB.Connection
  @doc false
  defdelegate stream(conn, sql, params, opts), to: Ecto.Adapters.DuckDB.Connection
end
