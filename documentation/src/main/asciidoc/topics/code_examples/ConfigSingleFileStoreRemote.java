import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
...

ConfigurationBuilder builder = new ConfigurationBuilder();
builder
  .remoteCache("mycache")
    .configuration("<infinispan><cache-container><distributed-cache name=\"mycache\"><persistence><file-store max-entries=\"5000\"/></persistence></distributed-cache></cache-container></infinispan>");
