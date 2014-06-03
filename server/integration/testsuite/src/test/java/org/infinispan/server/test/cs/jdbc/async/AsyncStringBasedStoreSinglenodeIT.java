package org.infinispan.server.test.cs.jdbc.async;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.cs.jdbc.AbstractJdbcStoreSinglenodeIT;
import org.infinispan.server.test.util.jdbc.DBServer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.infinispan.server.test.util.ITestUtils.createMemcachedClient;
import static org.junit.Assert.assertNotNull;

/**
 * A test for asynchronous jdbc cache store. We're not able to test individual attributes like flush-lock-timeout,
 * modification-queue-size, shutdown-timeout as we're outside the container.
 * <p/>
 * The only attribute that we check is thread-pool-size (checked in server log)
 * <p/>
 * The test proves that operations are performed asynchronously and do not block - the put operations should be fast. Waiting
 * for the entries to appear in the cache store takes longer time, though.
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 */
@Category(CacheStore.class)
public class AsyncStringBasedStoreSinglenodeIT extends AbstractJdbcStoreSinglenodeIT {

    private final String CONFIG_STRING_ASYNC = "testsuite/jdbc-string-async.xml";
    private final String TABLE_NAME_PREFIX = "STRING_ASYNC";
    private final String CACHE_NAME = "memcachedCache";
    private final String MANAGER_NAME = "local";

    @Test
    @WithRunningServer({@RunningServer(name = CONTAINER, config = CONFIG_STRING_ASYNC)})
    public void testPutRemove() throws Exception {
        mc = createMemcachedClient(server);

        int numEntries = 1000;
        String keyPrefix = "testPutRemove-k-";
        String valuePrefix = "testPutRemove-k-";
        // test PUT operation
        for (int i = 0; i != numEntries; i++) {
            mc.set(keyPrefix + i, valuePrefix + i);
        }
        for (int i = 0; i != numEntries; i++) {
            if (!dbServer.stringTable.exists()) {
                System.out.println("Table does not exist");
            }
            assertNotNull("Key " + keyPrefix + i + " was not found in DB in " + DBServer.TIMEOUT + " ms", dbServer.stringTable.getValueByKeyAwait(keyPrefix + i));
        }
        // test REMOVE operation
        for (int i = 0; i != numEntries; i++) {
            mc.delete(keyPrefix + i);
        }
        long limit = System.currentTimeMillis() + DBServer.TIMEOUT; //wait until
        while (!dbServer.stringTable.getAllRows().isEmpty()) {
            if (System.currentTimeMillis() > limit) {
                throw new RuntimeException("Timeout exceeded. The DB table is still not empty");
            }
        }
    }

    @Override
    protected String bucketTableName() {
        return null;
    }

    @Override
    protected String stringTableName() {
        return TABLE_NAME_PREFIX + "_" + CACHE_NAME;
    }

    @Override
    protected String managerName() {
        return MANAGER_NAME;
    }

    @Override
    protected String cacheName() {
        return CACHE_NAME;
    }

}
