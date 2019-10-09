package org.infinispan.api.mvcc.repeatable_read;

import java.util.stream.StreamSupport;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.WriteSkewWithPersistenceTest")
public class WriteSkewWithPersistenceTest extends WriteSkewTest {
   @Override
   protected ConfigurationBuilder createConfigurationBuilder() {
      ConfigurationBuilder configurationBuilder = super.createConfigurationBuilder();
      configurationBuilder.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(configurationBuilder.persistence()));
      configurationBuilder.clustering().hash().groups().enabled();
      return configurationBuilder;
   }

   @Override
   protected void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException {
      // Make sure that all entries are evicted from DC
      DataContainer<Object, Object> dataContainer = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      Object[] keys = StreamSupport.stream(dataContainer.spliterator(), false).map(InternalCacheEntry::getKey).toArray(Object[]::new);
      for (Object key : keys) {
         dataContainer.evict(key);
      }
      super.commit();
   }
}
