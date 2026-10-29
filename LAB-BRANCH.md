# lab/asg-2026-10-29

Shared build ref for the ASG Mesh lab (lab nodes and Ethan's Mac build this one ref, no local merges).

- Base: Mesh-LLM/mesh-llm `origin/main` at `2809443345ea8134db62a8b433f5bb2bb5f1d123`
- Merged with `git merge --no-ff`:
  - PR 2620 (plugin restart, mesh snapshot, M1 host side) at head `1d96a0205ed77959485159d2110259fd32ccb35e`
  - PR 2649 (capsules 0.1.5 pin + migration doc) at head `9c8cf6e800a71913533ef35133908fa6ff262ec8`
- PR 2536 (client nonce, M3) is already an ancestor of main (`24bbb8600bdb`); not added.
- Left out: PR 2654 (wallet refactor), PR 2644 (rustc warnings only).

## Rebuild

```
git fetch ethan lab/asg-2026-10-29 && git checkout FETCH_HEAD && just
```

Known failing test, same on plain main: `runtime::tests::auto_join::serve_auto_prefers_fast_join_probe_for_discovered_candidates`.
