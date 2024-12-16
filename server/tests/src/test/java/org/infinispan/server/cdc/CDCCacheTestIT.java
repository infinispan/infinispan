package org.infinispan.server.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.cdc.ChangeDataCaptureIT.CDC_TABLE_NAME;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfigurationBuilder;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

@org.infinispan.server.test.core.tags.Database
public class CDCCacheTestIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ChangeDataCaptureIT.SERVERS;

   @DatabaseTest
   public void testCacheWithCDCEnabled(Database database) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);
      builder.enabled(true);
      builder.connectionPool()
            .username(database.username())
            .password(database.password())
            .connectionUrl(database.jdbcUrl())
            .driverClass(database.driverClassName());
      // Set table name since cache name is custom.
      builder.table().name(CDC_TABLE_NAME);

      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withServerConfiguration(cb)
            .create();

      // After adding a CDC engine that populates the cache, this should fail.
      assertThat(cache.get("1")).isNull();
   }
}
