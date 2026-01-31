defmodule TestApp.Schemas.User do
  @moduledoc """
  User schema for demonstrating basic CRUD operations.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "users" do
    field(:name, :string)
    field(:email, :string)
    field(:active, :boolean, default: true)
  end

  def changeset(user, attrs) do
    user
    |> cast(attrs, [:id, :name, :email, :active])
    |> validate_required([:name, :email])
  end
end
