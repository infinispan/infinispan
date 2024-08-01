package org.infinispan.xsite.irac.persistence;

import java.lang.reflect.Method;
import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "irac.persistence.IracAsyncStoreTest")
public class IracAsyncStoreTest extends AbstractMultipleSitesTest {

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      var builder = super.defaultConfigurationForSite(siteIndex);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName("site-" + siteIndex)
            .shared(true)
            .async().enable();
      builder.sites()
            .addBackup().site(siteIndex == 0 ? siteName(1) : siteName(0))
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      builder.clustering().hash().numSegments(16);
      return builder;
   }

   public void testWrite(Method method) {
      var key = TestingUtil.k(method);
      cache(0, 0).put(key, "value");

      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value", cache.get(key)));
      eventuallyAssertInAllSitesAndCaches(cache -> storeForCache(cache).contains(key));
   }

   public void testRemove(Method method) {
      var key = TestingUtil.k(method);
      cache(0, 0).put(key, "value");

      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals("value", cache.get(key)));
      eventuallyAssertInAllSitesAndCaches(cache -> storeForCache(cache).contains(key));

      cache(0, 0).remove(key);
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.isNull(cache.get(key)));
      eventuallyAssertInAllSitesAndCaches(cache -> !storeForCache(cache).contains(key));
   }

   private static <K, V> WaitDelegatingNonBlockingStore<K, V> storeForCache(Cache<K, V> cache) {
      return TestingUtil.getFirstStoreWait(cache);
   }
}
