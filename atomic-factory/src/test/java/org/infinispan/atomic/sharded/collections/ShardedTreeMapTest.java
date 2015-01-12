package org.infinispan.atomic.sharded.collections;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicObjectFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import java.util.*;

import static org.infinispan.atomic.Utils.assertOnAllCaches;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Pierre Sutra
 * @since 7.2
 * *
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
        assertEquals(NCALLS,map.size());
        assertEquals(NCALLS/2,map.subMap(0,NCALLS/2).size());

        SortedMap<Integer,Integer> map2 = factory.getInstanceOf(ShardedTreeMap.class,"test2",false,null,false,3);
        Map<Integer,Integer> toAdd = new HashMap<Integer, Integer>();
        for(int i=0; i<NCALLS; i++) {
            toAdd.put(i, i);
            toAdd.get(i);
        }
        map2.putAll(toAdd);
        assertEquals(NCALLS,map2.size());
        assertEquals(map2.lastKey(),Integer.valueOf(NCALLS-1));

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
        assertOnAllCaches(caches,"k1", "value");
    }

}
