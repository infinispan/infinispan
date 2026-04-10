// Retrieve the marshaller registry
MarshallerRegistry registry = cacheManager.getGlobalComponentRegistry()
      .getComponent(MarshallerRegistry.class);

// Get a named marshaller
Marshaller customMarshaller = registry.getMarshaller("myCustomMarshaller");

// Or use with MediaType for automatic marshalling
MediaType mediaType = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("myCustomMarshaller");

// The DefaultTranscoder will automatically use the named marshaller
// when transcoding to/from this media type
cache.getAdvancedCache().withMediaType(mediaType, mediaType).put(key, value);
