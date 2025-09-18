/**
 * Register generated Protobuf schema with {brandname} Server.
 * This requires the RemoteCacheManager to be initialized.
 *
 * @param schema The schema to be registered. Can be a {@link GeneratedSchema } or a programmatic one.
 * See {@link SerializationContextInitialiser}
 */
private void registerSchemas(Schema schema) {
  RemoteSchemasAdmin schemas = remoteCacheManager.administration().schemas();
  schemas.create(schema);

  // Ensure the registered Protobuf schemas do not contain errors.
  // Throw an exception if errors exist.
  Optional<String> error = schemas.retrieveError(schema.getName())
  if (error.isPresent()) {
    throw new IllegalStateException("The schema contains an error: " + error.get() + "\nSchema :\n" + schema.getName());
  }
}
