package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.IckleFilterAndConverterLocalTest")
public class IckleFilterAndConverterLocalTest extends IckleFilterAndConverterDistTest {

   public IckleFilterAndConverterLocalTest() {
      super(1);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfgBuilder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      createClusteredCaches(numNodes, cfgBuilder);
   }
}
