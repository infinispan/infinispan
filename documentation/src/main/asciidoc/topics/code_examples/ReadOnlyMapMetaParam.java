ReadOnlyMap<String, String> readOnlyMap = ...

CompletableFuture<String> readFuture = readOnlyMap.eval("key1", view -> {
   // If caller depends on the typed information, this is not an ideal way to retrieve it
   // If the caller does not depend on the specific type, this works just fine.
   Optional<MetaEntryVersion> version = view.findMetaParam(MetaEntryVersion.class);
   return view.get();
});
