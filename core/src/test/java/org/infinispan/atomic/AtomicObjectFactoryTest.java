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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

@Test(groups = "functional", testName = "distexec.AtomicObjectFactoryTest")
public class AtomicObjectFactoryTest extends MultipleCacheManagersTest {

    private static int ncalls = 500;
    private static int ncaches = 4;
    private static List<Cache> caches = new ArrayList<Cache>();

    private static Log log = LogFactory.getLog(AtomicObjectFactory.class);

    public void testBasics() throws  Exception{

        EmbeddedCacheManager cacheManager = cacheManagers.iterator().next();
        Cache cache = cacheManager.getCache();
        AtomicObjectFactory factory = new AtomicObjectFactory(cache);

        // 1 - Basic Usage
        Set<String> set = (Set)factory.getOrCreateInstanceOf(HashSet.class, "set");
        set.add("smthing");
        assert set.contains("smthing");

        // 2 - Persistence
        factory.disposeInstanceOf(HashSet.class, "set", true);
        set = (Set<String>)factory.getOrCreateInstanceOf(HashSet.class, "set");
        assert set.contains("smthing");

        // 3 - Optimistic execution
        ArrayList<String> list = (ArrayList<String>)factory.getOrCreateInstanceOf(ArrayList.class, "list",true);
        assert !list.contains("foo");
        assert !cache.containsKey("list");

    }

    public void testAtomicTypeFactory() throws Exception {

        ExecutorService service = Executors.newCachedThreadPool();
        List<ArrayList> lists = new ArrayList<ArrayList>();
        List<AtomicObjectFactory> factories = new ArrayList<AtomicObjectFactory>();
        List<Future<Object>>  futures = new ArrayList<Future<Object>>();

        AtomicObjectFactory factory;
        ArrayList list;
        for(EmbeddedCacheManager manager: cacheManagers){
            Cache cache = manager.getCache();
            caches.add(cache);
            factory = new AtomicObjectFactory(cache);
            factories.add(factory);
            list = (ArrayList) factory.getOrCreateInstanceOf(ArrayList.class, "array");
            lists.add(list);
        }

        initAndTest();

        for(ArrayList l : lists){
            futures.add(service.submit(new ExerciceAtomicArrayTask(l, ncalls)));
        }

        for(Future future : futures){
            future.get();
        }

        int hash = factories.get(0).getHash();
        for(AtomicObjectFactory f : factories){
            assert f.getHash() == hash;
        }
        log.debug("Success" + hash);

    }

    @Override
    protected void createCacheManagers() throws Throwable {
        ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
        TransportFlags flags = new TransportFlags();
        createClusteredCaches(ncaches, builder, flags);
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

    private class ExerciceAtomicArrayTask implements Callable<Object>{

        private int ncalls;
        private ArrayList list;

        public ExerciceAtomicArrayTask(ArrayList l, int n){
            ncalls = n;
            list = l;
        }

        @Override
        public Object call() throws Exception {
            for(int i=0; i<ncalls;i++){
                Boolean result = list.add(i);
            }
            return  new Object();
        }
    }


}

