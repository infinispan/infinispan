package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.infinispan.atomic.Utils.assertOnAllCaches;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Pierre Sutra
 * @since 7.2
 * *
 */
@Test(groups = "functional", testName = "AtomicObjectFactoryTest")
public class AtomicObjectFactoryTest extends MultipleCacheManagersTest {

    private static int NCALLS= 1000;
    private static int NCACHES = 2;
    private static List<Cache> caches = new ArrayList<Cache>();

    private static Log log = LogFactory.getLog(AtomicObjectFactory.class);


    @Test(enabled = true)
    public void basicUsageTest() throws  Exception{

        EmbeddedCacheManager cacheManager = cacheManagers.iterator().next();
        Cache cache = cacheManager.getCache();
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        // 1 - Basic Usage
        Set<String> set = factory.getInstanceOf(HashSet.class, "set");
        set.add("smthing");
        assertTrue(set.contains("smthing"));
        assert set.size()==1;

        // 2 - Persistence
        factory.disposeInstanceOf(HashSet.class, "set", true);
        set = factory.getInstanceOf(HashSet.class, "set", false, null, false);
        assertTrue(set.contains("smthing"));

        // 3 - Optimistic execution
        ArrayList list = factory.getInstanceOf(ArrayList.class, "list", true);
        assertTrue(!list.contains("foo"));
        assertTrue(!cache.containsKey("list"));

    }

    @Test(enabled = true)
    public void basicPerformanceTest() throws Exception{

        EmbeddedCacheManager cacheManager = cacheManagers.iterator().next();
        Cache cache = cacheManager.getCache();
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        Map map = (Map) factory.getInstanceOf(HashMap.class, "map", true);

        for(int i=0; i<NCALLS*10;i++){
            map.containsKey("1");
        }
        long start = System.currentTimeMillis();
        for(int i=0; i<NCALLS*10;i++){
            map.containsKey("1");
        }

        log.debug(System.currentTimeMillis() - start);

    }

    @Test(enabled = true)
    public void distributedCacheTest() throws Exception {

        ExecutorService service = Executors.newCachedThreadPool();
        List<HashSet> sets = new ArrayList<HashSet>();
        List<AtomicObjectFactory> factories = new ArrayList<AtomicObjectFactory>();
        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

        AtomicObjectFactory factory;
        HashSet set;
        for(EmbeddedCacheManager manager: cacheManagers){
            Cache cache = manager.getCache();
            caches.add(cache);
            factory = new AtomicObjectFactory(cache);
            factories.add(factory);
            set = factory.getInstanceOf(HashSet.class, "aset", false, null, false);
            set.add(-1); // to synchronize the copies
            sets.add(set);
        }

        for(Set s : sets){
            futures.add(service.submit(new ExerciseAtomicSetTask(s, NCALLS)));
        }

        Integer total = 0;
        for(Future<Integer> future : futures){
            total += future.get();
        }

        assertEquals("obtained = " + total + "; espected = " + (NCALLS), Integer.valueOf(NCALLS), total);

    }

    @Test(enabled = true)
    public void distributedPersistenceTest() throws Exception {

        Iterator<EmbeddedCacheManager> it = cacheManagers.iterator();
        EmbeddedCacheManager manager1 = it.next();
        EmbeddedCacheManager manager2 = it.next();
        AtomicObjectFactory factory1, factory2;
        Cache cache1, cache2;
        HashSet set1, set2;

        cache1 = manager1.getCache();
        factory1 = new AtomicObjectFactory(cache1);
        set1 = factory1.getInstanceOf(HashSet.class, "persist");
        set1.add("smthing");
        factory1.disposeInstanceOf(HashSet.class,"persist",true);

        cache2 = manager2.getCache();
        factory2 = new AtomicObjectFactory(cache2);
        set2 = factory2.getInstanceOf(HashSet.class, "persist", true, null, false);
        assertTrue(set2.contains("smthing"));

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


    //
    // INNER CLASSES
    //

    private class ExerciseAtomicSetTask implements Callable<Integer>{

        private int ncalls;
        private Set set;

        public ExerciseAtomicSetTask(Set s, int n){
            ncalls = n;
            set = s;
        }

        @Override
        public Integer call() throws Exception {
            int ret = 0;
            for(int i=0; i<ncalls;i++){
                boolean r = set.add(i);
                if(r){
                    ret ++;
                }
            }
            return  ret;
        }
    }


}

