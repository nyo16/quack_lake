defmodule QuackLake.ErrorTest do
  use ExUnit.Case, async: true

  alias QuackLake.Error

  describe "exception/1 with keyword list" do
    test "creates error with message" do
      error = Error.exception(message: "Something went wrong")

      assert error.message == "Something went wrong"
      assert error.reason == nil
    end

    test "creates error with message and reason" do
      error = Error.exception(message: "Connection failed", reason: :timeout)

      assert error.message == "Connection failed"
      assert error.reason == :timeout
    end

    test "uses default message when not provided" do
      error = Error.exception(reason: :timeout)

      assert error.message == "QuackLake error"
      assert error.reason == :timeout
    end

    test "uses default message for empty opts" do
      error = Error.exception([])

      assert error.message == "QuackLake error"
      assert error.reason == nil
    end
  end

  describe "exception/1 with binary" do
    test "creates error with message only" do
      error = Error.exception("Something went wrong")

      assert error.message == "Something went wrong"
      assert error.reason == nil
    end
  end

  describe "wrap/1" do
    test "wraps binary error tuple" do
      error = Error.wrap({:error, "Connection refused"})

      assert error.message == "Connection refused"
      assert error.reason == "Connection refused"
    end

    test "wraps non-binary error tuple" do
      error = Error.wrap({:error, {:timeout, 5000}})

      assert error.message == "{:timeout, 5000}"
      assert error.reason == {:timeout, 5000}
    end

    test "wraps arbitrary term" do
      error = Error.wrap(:unexpected_value)

      assert error.message == ":unexpected_value"
      assert error.reason == :unexpected_value
    end

    test "wraps complex struct" do
      term = %{type: :error, code: 500}
      error = Error.wrap(term)

      assert error.message == inspect(term)
      assert error.reason == term
    end
  end

  describe "Exception behaviour" do
    test "can be raised with message" do
      assert_raise Error, "Test error", fn ->
        raise Error, "Test error"
      end
    end

    test "can be raised with options" do
      assert_raise Error, "Custom message", fn ->
        raise Error, message: "Custom message", reason: :test
      end
    end

    test "message/1 returns the message" do
      error = Error.exception(message: "Test error")

      assert Exception.message(error) == "Test error"
    end
  end
end
