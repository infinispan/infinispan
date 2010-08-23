package org.infinispan.api;

import org.infinispan.config.ConfigurationException;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * This is related to  https://jira.jboss.org/jira/browse/ISPN-83
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "api.NoFlushFailureTest")
public class NoFlushFailureTest {

   CacheContainer cm1;
   private static final String FILE = "configs/no-flush.xml";

   @Test (expectedExceptions = ConfigurationException.class)
   public void simpleTest() throws Exception {
      cm1 = TestCacheManagerFactory.fromXml(FILE);
      cm1.getCache();
   }
}


