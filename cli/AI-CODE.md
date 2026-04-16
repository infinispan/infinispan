# CLI Module Instructions

- The CLI uses Aesh for command parsing.
- Elytron credential store requires registering both `WildFlyElytronCredentialStoreProvider` and `WildFlyElytronPasswordProvider`.
- `Properties.stringPropertyNames()` returns an unmodifiable set in newer Java — use `stream().filter().toList()` then `forEach(props::remove)`.
- `KeyStoreCredentialStore.retrieve()` requires 5 parameters: alias, class, algorithmName, parameterSpec, protectionParameter.
- Avoid naming inner classes `Set` — it conflicts with `java.util.Set`. Use alternative names like `SetBookmark`.
