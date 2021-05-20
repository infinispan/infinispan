ConfigurationBuilder builder = new ConfigurationBuilder();
// Return previous values for keys for invocations for a specific cache.
builder.remoteCache("mycache")
       .forceReturnValues(true);
