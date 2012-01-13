package org.infinispan.tx;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test(groups = "functional", testName = "tx.BatchingAndEnlistment2Test")
public class BatchingAndEnlistment2Test extends BatchingAndEnlistmentTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = new ConfigurationBuilder().invocationBatching().enable().build();
      return new DefaultCacheManager(c);
   }
}

