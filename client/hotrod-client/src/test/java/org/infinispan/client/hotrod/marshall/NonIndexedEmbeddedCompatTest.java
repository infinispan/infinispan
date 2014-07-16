package org.infinispan.client.hotrod.marshall;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests compatibility between remote query and embedded mode. Do not enable indexing for query.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.marshall.NonIndexedEmbeddedCompatTest", groups = "functional")
@CleanupAfterMethod
public class NonIndexedEmbeddedCompatTest extends EmbeddedCompatTest {

   @Override
   protected ConfigurationBuilder createConfigBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.compatibility().enable().marshaller(new CompatibilityProtoStreamMarshaller());
      return builder;
   }

   @Test(enabled = false)
   @Override
   public void testEmbeddedQuery() throws Exception {
      // this would only make sense for Lucene based query
   }
}
