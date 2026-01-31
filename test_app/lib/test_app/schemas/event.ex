defmodule TestApp.Schemas.Event do
  @moduledoc """
  Event schema for demonstrating time travel features.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :integer, autogenerate: false}
  schema "events" do
    field(:user_id, :integer)
    field(:event_type, :string)
    field(:payload, :string)
    field(:occurred_at, :utc_datetime)
  end

  def changeset(event, attrs) do
    event
    |> cast(attrs, [:id, :user_id, :event_type, :payload, :occurred_at])
    |> validate_required([:event_type])
  end
end
