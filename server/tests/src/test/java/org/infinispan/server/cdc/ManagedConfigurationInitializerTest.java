package org.infinispan.server.cdc;

import static org.infinispan.server.cdc.ChangeDataCaptureIT.CDC_TABLE_NAME;

import org.infinispan.cdc.configuration.ChangeDataCaptureConfigurationBuilder;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.RegisterExtension;

@org.infinispan.server.test.core.tags.Database
public class ManagedConfigurationInitializerTest {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ChangeDataCaptureIT.SERVERS;

   @DatabaseTest
   public void testDataSourceConfiguration(Database database) {
      Assumptions.assumeTrue(ChangeDataCaptureIT.isDatabaseVendorSupported(database), "Vendor not supported: " + database.getType());

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      ChangeDataCaptureConfigurationBuilder builder = cb.addModule(ChangeDataCaptureConfigurationBuilder.class);
      // Enable and adds only database configuration.
      builder.enabled(true);
      builder.dataSource().jndiUrl("jdbc/" + database.getType());
      // Set table name since cache name is custom.
      builder.table().name(CDC_TABLE_NAME);

      // Ensure cache is working.
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(cb).create();
      cache.get("key");
   }
}
