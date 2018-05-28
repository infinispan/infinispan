package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author vjuranek
 * @since 9.2
 */
@Test(groups = "functional", testName = "scripting.ScriptingDataStoresTest")
@CleanupAfterMethod
public class ScriptingDataStoresTest extends AbstractScriptingTest {

   static final String CACHE_NAME = "script-exec";
   protected StorageType storageType;

   @Factory
   public Object[] factory() {
      return new Object[]{
            new ScriptingDataStoresTest().withStorageType(StorageType.OFF_HEAP),
            new ScriptingDataStoresTest().withStorageType(StorageType.BINARY),
            new ScriptingDataStoresTest().withStorageType(StorageType.OBJECT),
      };
   }

   ScriptingDataStoresTest withStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   protected String[] getScripts() {
      return new String[]{"test.js", "testExecWithoutProp.js"};
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration conf = new ConfigurationBuilder().memory().storageType(storageType).build();
      return new DefaultCacheManager(conf);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      cacheManager.defineConfiguration(CACHE_NAME, new ConfigurationBuilder().build());
   }

   @Override
   protected void clearContent() {
      cacheManager.getCache().clear();
   }

   public void testScriptWithParam() throws Exception {
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get(1000, TimeUnit.MILLISECONDS);
      assertEquals("a", result);
      assertEquals("a", cacheManager.getCache(CACHE_NAME).get("a"));
   }

   public void testScriptWithoutParam() throws Exception {
      String value = "javaValue";
      String key = "processValue";
      cacheManager.getCache(CACHE_NAME).put(key, value);

      scriptingManager.runScript("testExecWithoutProp.js").get(1000, TimeUnit.MILLISECONDS);
      assertEquals(value + ":additionFromJavascript", cacheManager.getCache(CACHE_NAME).get(key));
   }
}
