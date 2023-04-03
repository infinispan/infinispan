import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

ConfigurationBuilder builder = new ConfigurationBuilder();
builder.addServer()
   .host("127.0.0.1")
   .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
   .disableTracingPropagation();
