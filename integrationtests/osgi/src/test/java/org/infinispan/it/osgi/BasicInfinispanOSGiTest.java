package org.infinispan.it.osgi;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.it.osgi.util.CustomPaxExamRunner;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

/**
 * @author mgencur
 */
@RunWith(CustomPaxExamRunner.class)
@ExamReactorStrategy(PerSuite.class)
@Category(PerSuite.class)
public class BasicInfinispanOSGiTest extends BaseInfinispanCoreOSGiTest {

   @Test
   public void testCustomIspnConfigFile() throws IOException {
      URL configURL = BasicInfinispanOSGiTest.class.getClassLoader().getResource("infinispan.xml");
      EmbeddedCacheManager cacheManager = new DefaultCacheManager(configURL.openStream());
      cacheManager.defineConfiguration("default", new ConfigurationBuilder().build());
      try {
         Cache<String, String> cache = cacheManager.getCache("default");
         cache.put("k1", "v1");
         assertEquals("v1", cache.get("k1"));
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   @Override
   protected void createCacheManagers() {
      //not used
   }

}
