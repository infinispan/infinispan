package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;

@Test(groups = "functional", testName = "distexec.AtomicObjectFactoryTest")
public class AtomicObjectFactoryTest extends MultipleCacheManagersTest {

    private static int NCALLS= 1000;
    private static int NCACHES = 4;
    private static List<Cache> caches = new ArrayList<Cache>();

    private static Log log = LogFactory.getLog(AtomicObjectFactory.class);

    public void basicUsageTest() throws  Exception{

        EmbeddedCacheManager cacheManager = cacheManagers.iterator().next();
        Cache cache = cacheManager.getCache();
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        // 1 - Basic Usage
        Set<String> set = (Set)factory.getOrCreateInstanceOf(HashSet.class, "set");
        set.add("smthing");
        assert set.contains("smthing");
        assert set.size()==1;

        // 2 - Persistence
        factory.disposeInstanceOf(HashSet.class, "set", true);
        set = (Set<String>)factory.getOrCreateInstanceOf(HashSet.class, "set",false,null,false);
        assert set.contains("smthing");

        // 3 - Optimistic execution
        ArrayList<String> list = (ArrayList<String>)factory.getOrCreateInstanceOf(ArrayList.class, "list",true);
        assert !list.contains("foo");
        assert !cache.containsKey("list");

    }

    public void basicPerformanceTest() throws Exception{

        EmbeddedCacheManager cacheManager = cacheManagers.iterator().next();
        Cache cache = cacheManager.getCache();
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        Map map = (Map) factory.getOrCreateInstanceOf(HashMap.class, "set",true);

        for(int i=0; i<NCALLS*10;i++){
            map.containsKey("1");
        }
        long start = System.currentTimeMillis();
        for(int i=0; i<NCALLS*10;i++){
            map.containsKey("1");
        }

        log.debug(System.currentTimeMillis() - start);

    }

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
            set = (HashSet) factory.getOrCreateInstanceOf(HashSet.class, "set");
            sets.add(set);
        }

        initAndTest();

        for(Set s : sets){
            futures.add(service.submit(new ExerciceAtomicSetTask(s, NCALLS)));
        }

        Integer total = 0;
        for(Future<Integer> future : futures){
            total += future.get();
        }

        assert total == (NCALLS *(NCACHES-1)) : "obtained = "+total+"; espected = "+ (NCALLS * (NCACHES-1));

        int hash = factories.get(0).hashCode();
        for(AtomicObjectFactory f : factories){
            assert f.hashCode() == hash;
        }

    }

    public void distributedPersistenceTest() throws Exception {

        Iterator<EmbeddedCacheManager> it = cacheManagers.iterator();
        EmbeddedCacheManager manager1 = it.next();
        EmbeddedCacheManager manager2 = it.next();
        AtomicObjectFactory factory1, factory2;
        Cache cache1, cache2;
        HashSet set1, set2;

        cache1 = manager1.getCache();
        factory1 = new AtomicObjectFactory(cache1);
        set1 = (HashSet) factory1.getOrCreateInstanceOf(HashSet.class, "set");
        set1.add("smthing");


        cache2 = manager2.getCache();
        factory2 = new AtomicObjectFactory(cache2);
        set2 = (HashSet) factory2.getOrCreateInstanceOf(HashSet.class, "set",true,null,false);
        assert set2.contains("smthing");

    }

    @Override
    protected void createCacheManagers() throws Throwable {
        ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
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

    //
    // INNER CLASSES
    //

    private class ExerciceAtomicSetTask implements Callable<Integer>{

        private int ncalls;
        private Set set;

        public ExerciceAtomicSetTask(Set s, int n){
            ncalls = n;
            set = s;
        }

        @Override
        public Integer call() throws Exception {
            int ret = 0;
            for(int i=0; i<ncalls;i++){
                if( ! set.add(i) )
                    ret ++;
            }
            return  ret;
        }
    }


}

