package org.infinispan.atomic;

import javassist.CannotCompileException;
import javassist.NotFoundException;
import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Test(groups = "functional", testName = "distexec.AtomicTypeFactoryTest")
public class AtomicTypeFactoryTest extends MultipleCacheManagersTest {

    private static int ncalls = 500;
    private static int ncaches = 4;
    private static List<Cache> caches = new ArrayList<Cache>();

    private static Log log = LogFactory.getLog(AtomicTypeFactory.class);

    public void testAtomicTypeFactory() throws Exception {

        ExecutorService service = Executors.newCachedThreadPool();
        List<ArrayList> lists = new ArrayList<ArrayList>();
        List<AtomicTypeFactory> factories = new ArrayList<AtomicTypeFactory>();
        List<Future<Object>>  futures = new ArrayList<Future<Object>>();

        AtomicTypeFactory factory;
        ArrayList list;
        for(EmbeddedCacheManager manager: cacheManagers){
            Cache cache = manager.getCache();
            caches.add(cache);
            factory = new AtomicTypeFactory(cache);
            factories.add(factory);
            list = (ArrayList) factory.newInstanceOf(ArrayList.class, "array");
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
        for(AtomicTypeFactory f : factories){
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

