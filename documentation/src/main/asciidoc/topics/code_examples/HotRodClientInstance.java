org.infinispan.client.hotrod.configuration.ConfigurationBuilder cb
      = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
cb.marshaller(new org.infinispan.commons.marshall.ProtoStreamMarshaller())
  .statistics()
      .enable()
      .jmxDomain("org.infinispan")
  .addServer()
      .host("127.0.0.1")
      .port(11222);
RemoteCacheManager rmc = new RemoteCacheManager(cb.build());
