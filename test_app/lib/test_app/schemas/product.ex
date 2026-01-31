defmodule TestApp.Schemas.Product do
  @moduledoc """
  Product schema for demonstrating bulk insert operations.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "products" do
    field(:name, :string)
    field(:sku, :string)
    field(:price, :decimal)
    field(:quantity, :integer)
  end

  def changeset(product, attrs) do
    product
    |> cast(attrs, [:id, :name, :sku, :price, :quantity])
    |> validate_required([:name, :sku])
  end
end
