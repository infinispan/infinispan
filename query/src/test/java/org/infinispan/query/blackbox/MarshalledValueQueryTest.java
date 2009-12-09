package org.infinispan.query.blackbox;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 *
 * Test class such that it's possible to run queries on objects with the configuration setUseLazyDeserialization = true
 *
 * @author Navin Surtani
 * @since 4.0
 */


@Test(groups="functional")
public class MarshalledValueQueryTest extends LocalCacheTest {


   @Override
   protected CacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      c.setUseLazyDeserialization(true);
      Configuration.QueryConfigurationBean qcb = new Configuration.QueryConfigurationBean();
      qcb.setEnabled(true);
      c.setQueryConfigurationBean(qcb);
      return TestCacheManagerFactory.createCacheManager(c, true);
   }

}

