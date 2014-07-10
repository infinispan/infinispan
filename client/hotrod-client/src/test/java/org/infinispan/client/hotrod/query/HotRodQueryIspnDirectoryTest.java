package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test remote queries against Infinispan Directory provider.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.HotRodQueryIspnDirectoryTest", groups = "functional")
public class HotRodQueryIspnDirectoryTest extends HotRodQueryTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.indexing().addProperty("default.directory_provider", "infinispan");
      return builder;
   }
}
