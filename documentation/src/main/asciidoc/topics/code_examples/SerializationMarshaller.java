GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization()
       .marshaller(new JavaSerializationMarshaller())
       .whiteList()
       .addRegexps("org.infinispan.example.", "org.infinispan.concrete.SomeClass");
