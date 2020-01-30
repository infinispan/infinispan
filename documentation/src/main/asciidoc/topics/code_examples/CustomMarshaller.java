GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.serialization().marshaller(org.infinispan.example.marshall.CustomMarshaller.class)
      .addJavaSerialWhiteList("org.infinispan.example.*");
