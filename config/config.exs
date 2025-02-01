import Config

if Mix.env() == :test do
  config :junit_formatter,
    report_file: "ex_pg_query.junit.xml",
    print_report_file: true
end
