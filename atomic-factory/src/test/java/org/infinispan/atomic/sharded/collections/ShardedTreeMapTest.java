package org.infinispan.atomic.sharded.collections;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicObjectFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.util.*;


/**
 *
 * @author otrack
 * @since 7.0
 */
@Test(groups = "functional", testName = "ShardedTreeMapTest")
public class ShardedTreeMapTest extends MultipleCacheManagersTest {

    private static int NCALLS= 100;
    private static int NCACHES = 3;
    private static List<Cache> caches = new ArrayList<Cache>();

    @Test(enabled = true)
    public void basicUsageTest() throws  Exception{
        EmbeddedCacheManager cacheManager = cacheManagers.iterator().next();
        Cache cache = cacheManager.getCache();
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        SortedMap<Integer,Integer> map = factory.getInstanceOf(ShardedTreeMap.class,"test",false,null,false,3);
        for(int i=0; i<NCALLS; i++) {
            map.put(i,i);
            map.get(i);
        }
        factory.disposeInstanceOf(ShardedTreeMap.class,"test",true);
        map = factory.getInstanceOf(ShardedTreeMap.class,"test",false,null,false,3);
        int a = map.size();
        log.debug(a);
        assert map.size() == NCALLS;
        assert map.subMap(0,NCALLS/2).size()==NCALLS/2;

        SortedMap<Integer,Integer> map2 = factory.getInstanceOf(ShardedTreeMap.class,"test2",false,null,false,3);
        Map<Integer,Integer> toAdd = new HashMap<Integer, Integer>();
        for(int i=0; i<NCALLS; i++) {
            toAdd.put(i, i);
            toAdd.get(i);
        }
        map2.putAll(toAdd);
        assert map2.size() == NCALLS;
        assert map2.lastKey() == NCALLS-1;

    }

    //
    // HELPERS
    //

    @Override
    protected void createCacheManagers() throws Throwable {
        ConfigurationBuilder builder
                = AbstractCacheTest.getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
        TransportFlags flags = new TransportFlags();
        createClusteredCaches(NCACHES, builder, flags);
    }

    protected void initAndTest() {
        for (Cache<Object, String> c : caches) assert c.isEmpty();
        caches.iterator().next().put("k1", "value");
        assertOnAllCaches("k1", "value");
    }

    protected void assertOnAllCaches(Object key, String value) {
        for (Cache<Object, String> c : caches) {
            Object realVal = c.get(key);
            if (value == null) {
                assert realVal == null : "Expecting [" + key + "] to equal [" + value + "] on cache "+ c.toString();
            } else {
                assert value.equals(realVal) : "Expecting [" + key + "] to equal [" + value + "] on cache "+c.toString();
            }
        }
        // Allow some time for all ClusteredGetCommands to finish executing
        TestingUtil.sleepThread(1000);
    }

}
