GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization()
      .addNamedMarshaller("customJavaMarshaller", "org.infinispan.commons.marshall.JavaSerializationMarshaller")
      .addNamedMarshaller("myCustomMarshaller", new org.infinispan.example.MyCustomMarshaller());
