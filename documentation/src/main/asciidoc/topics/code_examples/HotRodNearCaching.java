import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;

ConfigurationBuilder builder = new ConfigurationBuilder();
builder.addServer()
    .host("127.0.0.1")
    .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
  .security().authentication()
    .username("username")
    .password("password")
    .realm("default")
    .saslMechanism("SCRAM-SHA-512")
  // Configure the connection pool for bloom filters.
  .connectionPool()
    .maxActive(1)
    .exhaustedAction(ExhaustedAction.WAIT);
// Configure near caching for specific caches
builder.remoteCache("books")
    .nearCacheMode(NearCacheMode.INVALIDATED)
    .nearCacheMaxEntries(100)
    .nearCacheUseBloomFilter(false);
builder.remoteCache("authors")
    .nearCacheMode(NearCacheMode.INVALIDATED)
    .nearCacheMaxEntries(200)
    .nearCacheUseBloomFilter(true);
