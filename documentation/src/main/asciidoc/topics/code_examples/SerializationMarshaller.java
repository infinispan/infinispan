GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.marshaller(JavaSerializationMarshaller.class)
      .addJavaSerialWhiteList("org.infinispan.example.*", "org.infinispan.concrete.SomeClass");
