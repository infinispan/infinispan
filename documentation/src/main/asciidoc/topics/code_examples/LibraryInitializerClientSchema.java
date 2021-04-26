/**
 * Register generated Protobuf schema with {brandname} Server.
 * This requires the RemoteCacheManager to be initialized.
 *
 * @param initializer The serialization context initializer for the schema.
 */
private void registerSchemas(SerializationContextInitializer initializer) {
  // Store schemas in the '___protobuf_metadata' cache to register them.
  // Using ProtobufMetadataManagerConstants might require the query dependency.
  final RemoteCache<String, String> protoMetadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
  // Add the generated schema to the cache.
  protoMetadataCache.put(initializer.getProtoFileName(), initializer.getProtoFile());

  // Ensure the registered Protobuf schemas do not contain errors.
  // Throw an exception if errors exist.
  String errors = protoMetadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
  if (errors != null) {
    throw new IllegalStateException("Some Protobuf schema files contain errors: " + errors + "\nSchema :\n" + initializer.getProtoFileName());
  }
}
