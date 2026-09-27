# Mocks
Enum.each(
  [Mix.Project, Nebulex.Cache.Registry, Nebulex.Time, Nebulex.Adapters.Local.Metadata],
  &Mimic.copy/1
)

# Nebulex dependency path
nbx_dep_path = Mix.Project.deps_paths()[:nebulex]

Code.require_file("#{nbx_dep_path}/test/support/fake_adapter.exs", __DIR__)
Code.require_file("#{nbx_dep_path}/test/support/cache_case.exs", __DIR__)

for file <- File.ls!("#{nbx_dep_path}/test/shared/cache") do
  Code.require_file("#{nbx_dep_path}/test/shared/cache/" <> file, __DIR__)
end

for file <- File.ls!("#{nbx_dep_path}/test/shared"), file != "cache" do
  Code.require_file("#{nbx_dep_path}/test/shared/" <> file, __DIR__)
end

for file <- File.ls!("test/shared"), not File.dir?("test/shared/" <> file) do
  Code.require_file("./shared/" <> file, __DIR__)
end

Code.require_file("support/test_cache.exs", __DIR__)

# Enable sleep mock to avoid test delay due to the sleep function (TTL tests)
:ok = Application.put_env(:nebulex, :sleep_mock, true)

# Start Telemetry
_ = Application.start(:telemetry)

# For tasks/generators testing
Mix.start()
Mix.shell(Mix.Shell.Process)

# Start ExUnit
ExUnit.start()
