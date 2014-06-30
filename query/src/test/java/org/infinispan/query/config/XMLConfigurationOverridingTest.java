package org.infinispan.query.config;

import org.junit.Assert;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.Serializable;

import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Tests verifying that the overriding of the configuration which is read from the configuration XML file is done
 * properly and later operations behaviour is as expected.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.config.XMLConfigurationOverridingTest")
public class XMLConfigurationOverridingTest extends AbstractInfinispanTest {

   private static final String simpleCacheName = "simpleLocalCache";

   public void testOverrideIndexing() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml("configs/named-cache-override-test.xml")) {
         @Override
         public void call() {
            Configuration cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertFalse(cnf.indexing().index().isEnabled());

            Configuration conf = new ConfigurationBuilder().indexing().index(Index.NONE)
                  .addProperty("default.directory_provider", "infinispan").build();

            cm.defineConfiguration(simpleCacheName, conf);

            cnf = cm.getCacheConfiguration(simpleCacheName);
            Assert.assertFalse(cnf.indexing().index().isEnabled());
            Assert.assertFalse(cnf.indexing().index().isLocalOnly());
            Assert.assertEquals("infinispan", cnf.indexing().properties().getProperty("default.directory_provider"));
            Assert.assertFalse(cm.getCacheNames().contains("LuceneIndexesMetadata"));

            cm.getCache(simpleCacheName).put("key0", new NonIndexedClass("value0"));

            Assert.assertFalse(cm.getCacheNames().contains("LuceneIndexesMetadata"));
         }
      });
   }

   class NonIndexedClass implements Serializable {
      public String description;

      NonIndexedClass(String description) {
         this.description = description;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         NonIndexedClass that = (NonIndexedClass) o;

         if (!description.equals(that.description)) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return description.hashCode();
      }
   }

}
