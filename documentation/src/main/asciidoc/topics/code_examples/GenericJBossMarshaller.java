GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization()
       .marshaller(new GenericJBossMarshaller())
       .allowList()
       .addRegexps("org.infinispan.example.", "org.infinispan.concrete.SomeClass");
