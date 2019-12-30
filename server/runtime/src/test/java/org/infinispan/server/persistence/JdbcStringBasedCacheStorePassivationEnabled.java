/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

/**
 * Tests String-based jdbc cache store under the following circumstances:
 * <p>
 * passivation == true --cache entries should get to the cache store only when evicted
 * preload == false --after server restart, entries should NOT be preloaded to the cache
 * purge == false --all entries should remain in the cache store after server restart
 * (must be false so that we can test preload)
 * <p>
 * Other attributes like singleton, shared, fetch-state do not make sense in single node cluster.
 *
 */
@Category(Persistence.class)
@RunWith(Parameterized.class)
public class JdbcStringBasedCacheStorePassivationEnabled {

    @ClassRule
    public static InfinispanServerRule SERVER = PersistenceIT.SERVERS;

    @ClassRule
    public static DatabaseServerRule DATABASE = new DatabaseServerRule(SERVER);

    @Rule
    public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        String[] databaseTypes = DatabaseServerRule.getDatabaseTypes("h2");
        List<Object[]> params = new ArrayList<>(databaseTypes.length);
        for (String databaseType : databaseTypes) {
            params.add(new Object[]{databaseType});
        }
        return params;
    }

    public JdbcStringBasedCacheStorePassivationEnabled(String databaseType) {
        DATABASE.setDatabaseType(databaseType);
    }

    @Test(timeout = 600000)
    public void testPassivateAfterEviction() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, true)
              .setEvition()
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());

        cache.put("k1", "v1");
        cache.put("k2", "v2");
        //not yet in store (eviction.size=2)
        assertTrue(table.tableExists() == false || table.getValueByKey(getEncodedKey("k1")) == null);
        assertTrue(table.tableExists() == false || table.getValueByKey(getEncodedKey("k2")) == null);
        cache.put("k3", "v3");
        assertEquals("v3", cache.get("k3"));
        //now some key is evicted and stored in store
        assertTrue(2 >= cache.size());
        assertEquals(1, table.countAllRows());
        //retrieve from store to cache and remove from store, cannot be found in the store
        cache.get("k1");
        assertTrue(2 >= cache.size());
        assertNull(table.getValueByKey(getEncodedKey("k1")));
        assertEquals(1, table.countAllRows());
    }

    @Test(timeout = 600000)
    public void testSoftRestartWithoutPreload() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, true)
              .setEvition()
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());

        cache.put("k1", "v1");
        cache.put("k2", "v2");
        //not yet in store (eviction.max-entries=2, LRU)
        assertTrue(table.tableExists() == false || table.getValueByKey(getEncodedKey("k1")) == null);
        assertTrue(table.tableExists() == false || table.getValueByKey(getEncodedKey("k2")) == null);
        cache.put("k3", "v3");
        //now some key is evicted and stored in store
        assertTrue(2 >= cache.size());
        assertEquals(1, table.countAllRows());
        SERVER.getServerDriver().restart(0); //soft stop should store all entries from cache to store
        cache = SERVER_TEST.hotrod().get().getRemoteCacheManager().getCache("default");
        // test preload==false
        assertEquals(0, cache.size());
        // test purge==false, entries should remain in the database after restart
        assertEquals(3, table.countAllRows());
        assertEquals("v1", cache.get(getEncodedKey("k1")));
    }

    /**
     * This test differs from the preceding expecting 1 entry in the DB
     * after fail-over instead of 3 when doing soft
     * restart.
     */
    @Test(timeout = 600000)
    public void testFailoverWithoutPreload() throws Exception {
        JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC)
              .createPersistenceConfiguration(DATABASE, true)
              .setEvition()
              .setLockingConfigurations();
        RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
        TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration(), jdbcUtil.getConfigurationBuilder());

        cache.put("k1", "v1");
        cache.put("k2", "v2");
        assertTrue(table.tableExists() == false || table.getValueByKey("k1") == null);
        assertTrue(table.tableExists() == false || table.getValueByKey("k2") == null);
        cache.put("k3", "v3");
        assertTrue(2 >= cache.size());
        assertEquals(1, table.countAllRows());
        SERVER.getServerDriver().restart(0); //soft stop should store all entries from cache to store
        assertEquals(0, cache.size());
        assertEquals(1, table.countAllRows());
        assertEquals("v1", cache.get("k1"));
    }

    private String getEncodedKey(String key) {
        return Base64.getEncoder().encodeToString(key.getBytes());
    }
}
