GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization().marshaller(JavaSerializationMarshaller.class)
      .addJavaSerialWhiteList("org.infinispan.example.*", "org.infinispan.concrete.SomeClass");
