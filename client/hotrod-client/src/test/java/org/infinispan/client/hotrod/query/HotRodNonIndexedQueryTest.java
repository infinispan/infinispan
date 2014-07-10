package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
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
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer().keyEquivalence(ByteArrayEquivalence.INSTANCE);
      return builder;
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000405:.*")
   @Override
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      super.testInvalidEmbeddedAttributeQuery();
   }
}
