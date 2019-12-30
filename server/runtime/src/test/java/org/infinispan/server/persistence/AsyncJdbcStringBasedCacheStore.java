package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.category.Persistence;
import org.infinispan.server.test.persistence.DatabaseServerRule;
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
    public static InfinispanServerRule SERVER = PersistenceIT.SERVERS;

    @ClassRule
    public static DatabaseServerRule DATABASE = new DatabaseServerRule(SERVER);

    @Rule
    public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

    public AsyncJdbcStringBasedCacheStore(String databaseType) {
        DATABASE.setDatabaseType(databaseType);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        String[] databaseTypes = DatabaseServerRule.getDatabaseTypes("h2");
        List<Object[]> params = new ArrayList<>(databaseTypes.length);
        for (String databaseType : databaseTypes) {
            params.add(new Object[]{databaseType});
        }
        return params;
    }

    @Test(timeout = 600000)
    public void testPutRemove() throws Exception {
        int numEntries = 1000;
        String keyPrefix = "testPutRemove-k-";
        String valuePrefix = "testPutRemove-k-";

        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, false)
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());

        // test PUT operation
        for (int i = 0; i != numEntries; i++) {
            cache.putAsync(keyPrefix+i, valuePrefix+i).toCompletableFuture().get();
        }

        if (!table.tableExists()) {
            fail("Table does not exist");
        }
        for (int i = 0; i != numEntries; i++) {
            assertCountRow(table.countAllRows(), numEntries);
            String encodedKey = Base64.getEncoder().encodeToString((keyPrefix + i).getBytes());
            String keyFromDatabase = table.getValueByKey(encodedKey);
            assertNotNull("Key " + keyPrefix + i + " was not found in DB" , keyFromDatabase);
            assertTrue(keyFromDatabase.contains(encodedKey));
        }

        //test REMOVE operation
        for (int i = 0; i != numEntries; i++) {
            cache.removeAsync(keyPrefix + i).toCompletableFuture().get();
        }
        assertCountRow(table.countAllRows(), 0);
    }

    public void assertCountRow(int result, int numberExpected) {
        assertEquals(numberExpected, result);
    }
}
