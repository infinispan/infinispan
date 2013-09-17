package org.infinispan.compatibility.adaptor52x;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.compatibility.loaders.Custom52xCacheStore;
import org.infinispan.compatibility.loaders.Custom52xCacheStoreConfig;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "compatibility.adaptor52x.Adaptor52xCustomLoaderTest")
public class Adaptor52xCustomLoaderTest extends BaseStoreTest {

   public static final String DIR = "__tmp_to_del___";
   private Cache<Object,Object> cache;
   private DefaultCacheManager dcm;
   protected String configurationFile;

   public Adaptor52xCustomLoaderTest() {
      configurationFile = "52x-custom-loader.xml";
   }

   @Override
   protected AdvancedLoadWriteStore createStore() throws Exception {
      TestingUtil.recursiveFileRemove(DIR);
      dcm = new DefaultCacheManager(configurationFile);
      return getAdvancedLoadWriteStore();
   }

   private AdvancedLoadWriteStore getAdvancedLoadWriteStore() {
      cache = dcm.getCache("customLoaderCache");
      return (AdvancedLoadWriteStore) TestingUtil.getFirstLoader(cache);
   }

   @AfterMethod
   @Override
   public void tearDown() throws CacheLoaderException {
      TestingUtil.killCacheManagers(dcm);
      TestingUtil.recursiveFileRemove(DIR);
   }

   public void testPreloadAndExpiry() {
      assert cache.getCacheConfiguration().persistence().preload();

      cache.put("k1", "v");
      cache.put("k2", "v", 111111, TimeUnit.MILLISECONDS);
      cache.put("k3", "v", -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
      cache.put("k4", "v", 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);

      assertCacheEntry(cache, "k1", "v", -1, -1);
      assertCacheEntry(cache, "k2", "v", 111111, -1);
      assertCacheEntry(cache, "k3", "v", -1, 222222);
      assertCacheEntry(cache, "k4", "v", 333333, 444444);
      cache.stop();

      cache.start();
      cl = getAdvancedLoadWriteStore();

      assertCacheEntry(cache, "k1", "v", -1, -1);
      assertCacheEntry(cache, "k2", "v", 111111, -1);
      assertCacheEntry(cache, "k3", "v", -1, 222222);
      assertCacheEntry(cache, "k4", "v", 333333, 444444);
   }

   private void assertCacheEntry(Cache cache, String key, String value, long lifespanMillis, long maxIdleMillis) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "No such entry for key " + key;
      assert Util.safeEquals(ice.getValue(), value) : ice.getValue() + " is not the same as " + value;
      assert ice.getLifespan() == lifespanMillis : "Lifespan " + ice.getLifespan() + " not the same as " + lifespanMillis;
      assert ice.getMaxIdle() == maxIdleMillis : "MaxIdle " + ice.getMaxIdle() + " not the same as " + maxIdleMillis;
      if (lifespanMillis > -1) assert ice.getCreated() > -1 : "Lifespan is set but created time is not";
      if (maxIdleMillis > -1) assert ice.getLastUsed() > -1 : "Max idle is set but last used is not";

   }


   public void testLocationIsCorrect() {
      Custom52xCacheStoreConfig config = ((Custom52xCacheStore) ((Adaptor52xStore) cl).getLoader()).getConfig();
      assertEquals(config.getLocation(), DIR);
   }

   @Override
   protected StreamingMarshaller getMarshaller() {
      return cache.getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }
}
