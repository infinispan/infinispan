import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
...

ConfigurationBuilder builder = new ConfigurationBuilder();
builder
  .remoteCache("mycache")
    .configuration("<distributed-cache name=\"mycache\"><persistence><file-store max-entries=\"5000\"/></persistence></distributed-cache>");
