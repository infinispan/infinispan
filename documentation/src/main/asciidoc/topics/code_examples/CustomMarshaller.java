GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.marshaller(org.infinispan.example.marshall.CustomMarshaller.class)
      .addJavaSerialWhiteList("org.infinispan.example.*");
