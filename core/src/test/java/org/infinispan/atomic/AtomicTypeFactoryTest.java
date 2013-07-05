package org.infinispan.atomic;

import javassist.CannotCompileException;
import javassist.NotFoundException;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;

@Test(groups = "functional", testName = "AtomicTypeFactoryTest")
public class AtomicTypeFactoryTest extends SingleCacheManagerTest {

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        Configuration c = getDefaultStandaloneConfig(false);
        CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
        EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
        cache = cm.getCache();
        return cm;
    }

    public void testAtomicTypeFactory() throws Exception {

		AtomicTypeFactory fact = new AtomicTypeFactory(cache);
		try {
			@SuppressWarnings("unchecked")
            ArrayList<Object> l = (ArrayList<Object>) fact.newInstanceOf(ArrayList.class, "0");
			l.add("1");
			log.trace(l.toString());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
        }

    }

}

