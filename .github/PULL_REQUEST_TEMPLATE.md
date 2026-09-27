# Pull request

## Summary

<!-- What changes, and why. One logical change per pull request. -->

## Type

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor / internal cleanup
- [ ] Documentation
- [ ] Benchmark / measurement only
- [ ] Build / CI / repository structure

## Verification

<!-- Commands you ran and their results. -->

- [ ] `dotnet build Sparc.slnx -c Release` is clean (warnings are errors)
- [ ] `dotnet test Sparc.slnx -c Release` passes
- [ ] `dotnet build Sparc.Samples.slnx -c Release` passes (if samples or public API changed)
- [ ] Benchmarks run for performance changes, with numbers in the summary
      (and `--regression` against a re-captured baseline where relevant)

## Notes

- [ ] Public API changes are documented (XML docs and `docs/`)
- [ ] Hot-path changes keep the allocation budget (`SteadyStateDoesNotAllocate`)
- [ ] Invariant changes are reflected in `docs/performance-invariants.md`
- [ ] No secrets, credentials or machine-specific artifacts are included
