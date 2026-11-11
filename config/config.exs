use Mix.Config

# config :clickhousex, codec: Clickhousex.Codec.RowBinary

if Mix.env() == :test do
  # The suite reaches the gzip path only with this on and a server profile that sets enable_http_compression.
  config :clickhousex, compress: true
end
