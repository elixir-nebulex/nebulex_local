# Used by "mix format"
locals_without_parens = [
  # Nebulex.Adapters.Local.QueryHelper
  match_spec: 1
]

[
  import_deps: [:nebulex],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test,benchmarks}/**/*.{ex,exs}"],
  line_length: 100,
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
