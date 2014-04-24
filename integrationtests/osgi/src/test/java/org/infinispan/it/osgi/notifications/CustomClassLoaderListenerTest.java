package org.infinispan.it.osgi.notifications;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.it.osgi.Osgi;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import static org.infinispan.it.osgi.util.IspnKarafOptions.*;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

/**
 *
 * @author mgencur
 */
@RunWith(PaxExam.class)
@Category(Osgi.class)
@ExamReactorStrategy(PerClass.class)
public class CustomClassLoaderListenerTest extends org.infinispan.notifications.cachelistener.CustomClassLoaderListenerTest {

   private CustomClassLoader ccl;

   @Configuration
   public Option[] config() throws Exception {
      return options(
            karafContainer(),
            featureIspnCoreDependencies(),
            featureIspnCorePlusTests(),
            junitBundles(),
            keepRuntimeFolder()
      );
   }

   @Before
   public void setUp() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class);
      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      if (cache == null) cache = cacheManager.getCache();
   }

   @After
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Test
   public void testCustomClassLoaderListener() throws Exception {
      super.testCustomClassLoaderListener();
   }

   public static class CustomClassLoader extends ClassLoader {
      public CustomClassLoader(ClassLoader parent) {
         super(parent);
      }
   }

}
