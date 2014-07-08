package org.infinispan.query.config;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "unit", testName = "query.config.DefaultConfigDeclarativeTest")
public class DeclarativeAutoConfigTest extends AbstractInfinispanTest {

   @Test
   public void testAutoConfig() throws IOException {
      withCacheManager(new CacheManagerCallable(
              TestCacheManagerFactory.fromXml("configuration-parsing-test.xml")) {
         @Override
         public void call() {
            Configuration cacheConfiguration = cm.getCacheConfiguration("repl-with-default");
            TypedProperties properties = cacheConfiguration.indexing().properties();
            assertFalse(properties.isEmpty());
            assertEquals(properties.getProperty("hibernate.search.default.exclusive_index_use"), "true");
            assertEquals(properties.getProperty("hibernate.search.default.reader.strategy"), "shared");
            assertEquals(properties.getProperty("hibernate.search.default.indexmanager"), "near-real-time");
            assertEquals(properties.getProperty("hibernate.search.default.directory_provider"), "filesystem");
            
            cacheConfiguration = cm.getCacheConfiguration("dist-with-default");
            properties = cacheConfiguration.indexing().properties();
            
            assertFalse(properties.isEmpty());
            assertEquals(properties.getProperty("hibernate.search.default.directory_provider"), "infinispan");
            assertEquals(properties.getProperty("hibernate.search.default.indexmanager"), "org.infinispan.query.indexmanager.InfinispanIndexManager");
            assertEquals(properties.getProperty("hibernate.search.default.exclusive_index_use"), "true");
            assertEquals(properties.getProperty("hibernate.search.default.reader.strategy"), "shared");


         }
      });
   }


}
