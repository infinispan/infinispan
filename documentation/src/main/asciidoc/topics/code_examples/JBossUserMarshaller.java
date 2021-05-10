GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization()
       .marshaller(new GenericJBossMarshaller())
       .whiteList()
       .addRegexps("org.infinispan.example.", "org.infinispan.concrete.SomeClass");
