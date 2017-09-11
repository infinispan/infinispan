package org.infinispan.atomic;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author Pierre Sutra
 * @since 7.2
 */
@Test(groups = "functional", testName = "AtomicObjectFactoryTest")
@CleanupAfterMethod
public class AtomicObjectFactoryTest extends MultipleCacheManagersTest {

    private static int NCALLS= 10000;
    private static int NCACHES = 2;

    private static Log log = LogFactory.getLog(AtomicObjectFactory.class);

    public void basicUsageTest() throws Exception {
        Cache<?, ?> cache = cache(0);
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        // 1 - Basic Usage
        Set<String> set = factory.getInstanceOf(HashSet.class, "set");
        set.add("smthing");
        assertTrue(set.contains("smthing"));
        assertEquals(1, set.size());

        // 2 - Persistence
        factory.disposeInstanceOf(HashSet.class, "set", true);
        set = factory.getInstanceOf(HashSet.class, "set", false, null, false);
        assertTrue(set.contains("smthing"));

        // 3 - Optimistic execution
        ArrayList<String> list = factory.getInstanceOf(ArrayList.class, "list", true);
        assertTrue(!list.contains("foo"));
        assertTrue(!cache.containsKey("list"));

    }

    public void basicPerformanceTest() throws Exception {
        Cache<?, ?> cache = cache(0);
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        Map<String, ?> map = factory.getInstanceOf(HashMap.class, "map", true);

        for(int i=0; i<NCALLS*10;i++){
            map.containsKey("1");
        }
        long start = System.currentTimeMillis();
        for(int i=0; i<NCALLS*10;i++){
            map.containsKey("1");
        }

        log.debug(System.currentTimeMillis() - start);

    }

    @Test(groups = "unstable", description = "ISPN-5530")
    public void distributedCacheTest() throws Exception {

        List<HashSet<Integer>> sets = new ArrayList<>();
        List<Future<Integer>> futures = new ArrayList<>();

        for(EmbeddedCacheManager manager: cacheManagers){
            Cache<?, ?> cache = manager.getCache();
            AtomicObjectFactory factory = new AtomicObjectFactory(cache);
            HashSet<Integer> set = factory.getInstanceOf(HashSet.class, "aset", false, null, false);
            set.add(-1); // to synchronize the copies
            sets.add(set);
        }

        for(Set<Integer> s : sets){
            futures.add(fork(new ExerciseAtomicSetTask(s, NCALLS)));
        }

        Integer total = 0;
        for(Future<Integer> future : futures){
            total += future.get();
        }

        assertEquals(NCALLS, total.intValue());

    }

    public void distributedPersistenceTest() throws Exception {

        Iterator<EmbeddedCacheManager> it = cacheManagers.iterator();
        EmbeddedCacheManager manager1 = it.next();
        EmbeddedCacheManager manager2 = it.next();
        AtomicObjectFactory factory1, factory2;
        Cache<?, ?> cache1, cache2;
        HashSet<String> set1, set2;

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
        createClusteredCaches(NCACHES, builder);
    }

    //
    // INNER CLASSES
    //

    private class ExerciseAtomicSetTask implements Callable<Integer> {

        private int ncalls;
        private Set<Integer> set;
        private Set<Integer> added;

        public ExerciseAtomicSetTask(Set<Integer> s, int n) {
            ncalls = n;
            set = s;
            added = new HashSet<>();
        }

        @Override
        public Integer call() throws Exception {
            int ret = 0;
            for (int i = 0; i < ncalls; i++) {
                boolean r = set.add(i);
                if (r) {
                    ret ++;
                    added.add(i);
                }
            }
            log.debugf("Thread %d added %s\n", Thread.currentThread().getId(), added);
            return  ret;
        }
    }


}
