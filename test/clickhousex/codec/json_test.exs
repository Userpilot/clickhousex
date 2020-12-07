defmodule Clickhousex.Codec.JSONTest do
  use ClickhouseCase

  alias Clickhousex.Codec.JSON
  alias Clickhousex.Result

  describe "decode_row" do
    for size <- [8, 16, 32, 64] do
      test "decodes UInt#{size}" do
        size = unquote(size)
        value = floor(:math.pow(2, size)) - 1
        row = ["#{value}"]
        column_types = ["UInt#{size}"]

        assert [^value] = JSON.decode_row(row, column_types)
      end

      test "decodes Int#{size}" do
        size = unquote(size)
        value = floor(:math.pow(2, size - 1)) - 1
        row = ["#{value}"]
        column_types = ["Int#{size}"]

        assert [^value] = JSON.decode_row(row, column_types)
      end
    end

    for size <- [32, 64] do
      test "decodes Float#{size}" do
        size = unquote(size)
        value = :math.pow(2, size - 1)
        row = [value]
        column_types = ["Float#{size}"]

        assert [^value] = JSON.decode_row(row, column_types)
      end
    end

    test "decodes uuid" do
      value = "f3e592bf-beba-411e-8a77-668ef76b1957"
      row = [value]
      column_types = ["UUID"]

      assert [^value] = JSON.decode_row(row, column_types)
    end

    test "decodes Date" do
      value = "1970-01-01"
      row = [value]
      column_types = ["Date"]

      assert [~D[1970-01-01]] == JSON.decode_row(row, column_types)
    end

    test "decodes DateTime" do
      value = "1970-01-01 00:00:00"
      row = [value]
      column_types = ["DateTime"]

      assert [~N[1970-01-01 00:00:00]] == JSON.decode_row(row, column_types)
    end
  end

  test "integration", ctx do
    create_statement = """
    CREATE TABLE {{table}} (
      u64_val UInt64,
      u32_val UInt32,
      u16_val UInt16,
      u8_val  UInt8,

      i64_val Int64,
      i32_val Int32,
      i16_val Int16,
      i8_val  Int8,

      f64_val Float64,
      f32_val Float32,

      string_val String,
      fixed_string_val FixedString(5),

      uuid_val UUID,

      date_val Date,
      date_time_val DateTime
    )

    ENGINE = Memory
    """

    {:ok, _} = schema(ctx, create_statement)

    date = Date.utc_today()
    datetime = DateTime.utc_now() |> DateTime.truncate(:second)

    row = [
      329,
      328,
      327,
      32,
      429,
      428,
      427,
      42,
      29.8,
      4.0,
      "This is long",
      "hello",
      "f3e592bf-beba-411e-8a77-668ef76b1957",
      date,
      datetime
    ]

    assert {:ok, %Result{command: :updated, num_rows: 1}} =
             insert(
               ctx,
               "INSERT INTO {{table}} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               row
             )

    assert {:ok, %Result{rows: rows}} = select_all(ctx)

    naive_datetime =
      datetime
      |> DateTime.to_naive()
      |> NaiveDateTime.truncate(:second)

    assert [
             [
               329,
               328,
               327,
               32,
               429,
               428,
               427,
               42,
               29.8,
               4.0,
               "This is long",
               "hello",
               "f3e592bf-beba-411e-8a77-668ef76b1957",
               ^date,
               ^naive_datetime
             ]
           ] = rows
  end
end
