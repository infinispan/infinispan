package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.query.HotRodNonIndexedQueryTest", groups = "functional")
public class HotRodNonIndexedQueryTest extends HotRodQueryTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      return new ConfigurationBuilder();
   }
}
