GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization()
       .marshaller(new JavaSerializationMarshaller())
       .allowList()
       .addRegexps("org.infinispan.example.", "org.infinispan.concrete.SomeClass");
