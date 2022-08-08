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

    test "decodes SimpleAggregateFunction(aggregate, Type)" do
      unsigned_int32 = 100
      datetime_iso = "2022-07-23 16:04:51"
      datetime64_iso = "2022-08-07 14:10:42.997835"
      array_of_nullables = ['5687', '481', nil, nil]

      row = [unsigned_int32, datetime_iso, datetime64_iso, array_of_nullables]

      column_types = [
        "SimpleAggregateFunction(sum, UInt32)",
        "SimpleAggregateFunction(min, DateTime)",
        "SimpleAggregateFunction(max, DateTime64(6, 'Etc/UTC'))",
        "SimpleAggregateFunction(any, Array(Nullable(String)))"
      ]

      assert [
               unsigned_int32,
               ~N[2022-07-23 16:04:51],
               ~N[2022-08-07 14:10:42.997835],
               array_of_nullables
             ] == JSON.decode_row(row, column_types)
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
      date_time_val DateTime,
      date_time_64_val DateTime64(6),

      simple_aggregate_datetime SimpleAggregateFunction(min, DateTime),
      simple_aggregate_datetime64 SimpleAggregateFunction(max, DateTime64(6, 'Etc/UTC'))
    )

    ENGINE = Memory
    """

    {:ok, _} = schema(ctx, create_statement)

    date = Date.utc_today()
    datetime = DateTime.utc_now() |> DateTime.truncate(:second)
    datetime64 = DateTime.utc_now()

    simple_aggregate_datetime64 = DateTime.utc_now() |> DateTime.add(-86_400, :second)
    simple_aggregate_datetime = DateTime.truncate(simple_aggregate_datetime64, :second)

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
      datetime,
      datetime64,
      simple_aggregate_datetime,
      simple_aggregate_datetime64
    ]

    assert {:ok, %Result{command: :updated, num_rows: 1}} =
             insert(
               ctx,
               "INSERT INTO {{table}} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               row
             )

    assert {:ok, %Result{rows: rows}} = select_all(ctx)

    naive_datetime = DateTime.to_naive(datetime)
    naive_datetime64 = DateTime.to_naive(datetime64)

    simple_aggregate_naive_datetime = DateTime.to_naive(simple_aggregate_datetime)
    simple_aggregate_naive_datetime64 = DateTime.to_naive(simple_aggregate_datetime64)

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
               ^naive_datetime,
               ^naive_datetime64,
               ^simple_aggregate_naive_datetime,
               ^simple_aggregate_naive_datetime64
             ]
           ] = rows
  end
end
