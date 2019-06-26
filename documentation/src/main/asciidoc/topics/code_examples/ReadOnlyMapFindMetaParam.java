class MetaEntryVersion<T> implements MetaParam.Writable<EntryVersion<T>> {
   ...
   public static <T> T type() { return (T) MetaEntryVersion.class; }
   ...
}

ReadOnlyMap<String, String> readOnlyMap = ...

CompletableFuture<String> readFuture = readOnlyMap.eval("key1", view -> {
   // The caller wants guarantees that the metadata parameter for version is numeric
   // e.g. to query the actual version information
   Optional<MetaEntryVersion<Long>> version = view.findMetaParam(MetaEntryVersion.type());
   return view.get();
});
