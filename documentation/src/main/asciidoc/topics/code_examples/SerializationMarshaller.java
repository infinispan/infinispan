GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
builder.serialization().marshaller(new JavaSerializationMarshaller())
      .addJavaSerialWhiteList("org.infinispan.example.*", "org.infinispan.concrete.SomeClass");
