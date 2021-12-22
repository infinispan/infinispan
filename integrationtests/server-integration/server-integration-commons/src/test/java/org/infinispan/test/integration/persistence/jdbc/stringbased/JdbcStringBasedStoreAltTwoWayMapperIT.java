package org.infinispan.test.integration.persistence.jdbc.stringbased;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.stringbased.TwoWayPersonKey2StringMapper;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JdbcStringBasedStoreAltTwoWayMapperIT extends JdbcStringBasedStoreAltMapperIT {

    @Override
    protected JdbcStringBasedStoreConfigurationBuilder createJdbcConfig(ConfigurationBuilder builder) {
        JdbcStringBasedStoreConfigurationBuilder storeBuilder = builder
                .persistence()
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .key2StringMapper(TwoWayPersonKey2StringMapper.class);
        return storeBuilder;
    }

    @Test
    public void testPurgeListenerIsNotified() throws Exception {
        InternalCacheEntry first = TestInternalCacheEntryFactory.create(MIRCEA, "val", 1000);
        cacheStore.write(MarshalledEntryUtil.create(first, marshaller));
        assertRowCount(1);
        Thread.sleep(1100);
        List<MarshallableEntry> purged = cacheStore.purge();
        assertEquals(1, purged.size());
        assertEquals(MIRCEA, purged.get(0).getKey());
        assertRowCount(0);
    }

}
