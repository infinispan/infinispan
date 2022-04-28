package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(Persistence.class)
@RunWith(Parameterized.class)
public class AsyncJdbcStringBasedCacheStore {

    @ClassRule
    public static InfinispanServerRule SERVERS = PersistenceIT.SERVERS;

    @Rule
    public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

    private final Database database;

    public AsyncJdbcStringBasedCacheStore(String databaseType) {
        this.database = PersistenceIT.DATABASE_LISTENER.getDatabase(databaseType);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        String[] databaseTypes = PersistenceIT.DATABASE_LISTENER.getDatabaseTypes();
        List<Object[]> params = new ArrayList<>(databaseTypes.length);
        for (String databaseType : databaseTypes) {
            params.add(new Object[]{databaseType});
        }
        return params;
    }

    @Test
    public void testPutRemove() throws Exception {
        int numEntries = 10;
        String keyPrefix = "testPutRemove-k-";
        String valuePrefix = "testPutRemove-k-";

        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
                .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil)) {
            // test PUT operation
            for (int i = 0; i < numEntries; i++) {
                cache.putAsync(keyPrefix+i, valuePrefix+i).toCompletableFuture().get();
            }

            assertCountRow(table.countAllRows(), numEntries);
            for (int i = 0; i < numEntries; i++) {
                String key = keyPrefix + i;
                String keyFromDatabase = table.getValueByKey(key);
                assertNotNull("Key " + key + " was not found in DB" , keyFromDatabase);
            }

            //test REMOVE operation
            for (int i = 0; i < numEntries; i++) {
                cache.removeAsync(keyPrefix + i).toCompletableFuture().get();
            }
            assertCountRow(table.countAllRows(), 0);
        }

    }

    public void assertCountRow(int result, int numberExpected) {
        assertEquals(numberExpected, result);
    }
}
