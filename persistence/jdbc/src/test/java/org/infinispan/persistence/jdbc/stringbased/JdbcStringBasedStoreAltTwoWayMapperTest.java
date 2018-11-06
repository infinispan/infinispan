package org.infinispan.persistence.jdbc.stringbased;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 */
@Test(groups = "functional", testName = "persistence.jdbc.stringbased.JdbcStringBasedStoreAltTwoWayMapperTest")
public class JdbcStringBasedStoreAltTwoWayMapperTest extends JdbcStringBasedStoreAltMapperTest {

   @Override
   protected JdbcStringBasedStoreConfigurationBuilder createJdbcConfig(ConfigurationBuilder builder) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .key2StringMapper(TwoWayPersonKey2StringMapper.class);
      return storeBuilder;
   }

   public void testPurgeListenerIsNotified() throws Exception {
      InternalCacheEntry first = TestInternalCacheEntryFactory.create(MIRCEA, "val", 1000);
      cacheStore.write(MarshalledEntryUtil.create(first, marshaller));
      assertRowCount(1);
      Thread.sleep(1100);
      AdvancedCacheWriter.PurgeListener purgeListener = mock(AdvancedCacheWriter.PurgeListener.class);
      cacheStore.purge(new WithinThreadExecutor(), purgeListener);
      verify(purgeListener).entryPurged(MIRCEA);
      assertRowCount(0);
   }
}
