package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.utils.ScriptingUtils;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.loadScript;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withScript;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * ExecTest.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@Test(groups = "functional", testName = "client.hotrod.ExecTest")
public class ExecTest extends MultiHotRodServersTest {
   private static final String SCRIPT_CACHE = "___script_cache";
   static final String REPL_CACHE = "R";
   static final String DIST_CACHE = "D";

   static final int NUM_SERVERS = 2;
   static final int SIZE = 20;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());
      defineInAll(REPL_CACHE, CacheMode.REPL_SYNC);
      defineInAll(DIST_CACHE, CacheMode.DIST_SYNC);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      clients.get(0).getCache(REPL_CACHE).clear();
      clients.get(0).getCache(DIST_CACHE).clear();
   }

   private void defineInAll(String cacheName, CacheMode mode) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(mode, true);
      builder.dataContainer()
            .keyEquivalence(new AnyServerEquivalence())
            .valueEquivalence(new AnyServerEquivalence())
            .compatibility().enable()
            .marshaller(new GenericJBossMarshaller());
      defineInAll(cacheName, builder);
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unknown task 'nonExistent\\.js'.*")
   public void testRemovingNonExistentScript() {
      clients.get(0).getCache().execute("nonExistent.js", new HashMap<>());
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testEmbeddedScriptRemoteExecution(String cacheName) throws IOException {
      withScript(manager(0), "/test.js", scriptName -> {
         populateCache(cacheName);
         assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
         Map<String, String> params = new HashMap<>();
         params.put("parameter", "guinness");
         Integer result = clients.get(0).getCache(cacheName).execute(scriptName, params);
         assertEquals(SIZE + 1, result.intValue());
         assertEquals("guinness", clients.get(0).getCache(cacheName).get("parameter"));
      });
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testRemoteScriptRemoteExecution(String cacheName) throws IOException {
      withScript(manager(0), "/test.js", scriptName -> {
         populateCache(cacheName);

         assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
         Map<String, String> params = new HashMap<>();
         params.put("parameter", "hoptimus prime");

         Integer result = clients.get(0).getCache(cacheName).execute(scriptName, params);
         assertEquals(SIZE + 1, result.intValue());
         assertEquals("hoptimus prime", clients.get(0).getCache(cacheName).get("parameter"));
      });
   }

   @Test(enabled = false, dataProvider = "CacheModeProvider", description = "Enable when ISPN-6300 is fixed.")
   public void testScriptExecutionWithPassingParams(CacheMode cacheMode) throws IOException {
      String cacheName = "testScriptExecutionWithPassingParams_" + cacheMode.toString();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence()).valueEquivalence(new AnyServerEquivalence()).compatibility().enable().marshaller(new GenericJBossMarshaller());
      defineInAll(cacheName, builder);
      try (InputStream is = this.getClass().getResourceAsStream("/distExec.js")) {
         String script = TestingUtil.loadFileAsString(is);
         manager(0).getCache(SCRIPT_CACHE).put("testScriptExecutionWithPassingParams.js", script);
      }
      populateCache(cacheName);

      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
      Map<String, String> params = new HashMap<>();
      params.put("a", "hoptimus prime");

      List<String> result = clients.get(0).getCache(cacheName).execute("testScriptExecutionWithPassingParams.js", params);
      assertEquals(SIZE + 1, client(0).getCache(cacheName).size());
      assertEquals("hoptimus prime", clients.get(0).getCache(cacheName).get("a"));

      assertEquals(2, result.size());
      assertTrue(result.contains(manager(0).getAddress()));
      assertTrue(result.contains(manager(1).getAddress()));
   }

   private void populateCache(String cacheName) {
      for (int i = 0; i < SIZE; i++)
         clients.get(i % NUM_SERVERS).getCache(cacheName).put(String.format("Key %d", i), String.format("Value %d", i));
   }

   @Test(enabled = false, dataProvider = "CacheModeProvider",
           description = "Disabling this test until the distributed scripts in DIST mode are fixed - ISPN-6173")
   public void testRemoteMapReduceWithStreams(CacheMode cacheMode) throws Exception {
      String cacheName = "testRemoteMapReduce_Streams_dist_" + cacheMode.toString();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence()).valueEquivalence(new AnyServerEquivalence())
              .compatibility().enable().marshaller(new GenericJBossMarshaller());
      defineInAll(cacheName, builder);
      waitForClusterToForm(cacheName);

      RemoteCache<String, String> cache = clients.get(0).getCache(cacheName);
      RemoteCache<String, String> scriptCache = clients.get(1).getCache(SCRIPT_CACHE);
      ScriptingUtils.loadData(cache, "/macbeth.txt");
      ScriptingManager scriptingManager = manager(0).getGlobalComponentRegistry().getComponent(ScriptingManager.class);
      loadScript("/wordCountStream_dist.js", scriptingManager, "wordCountStream_dist.js");

      ArrayList<Map<String, Long>> results = cache.execute("wordCountStream_dist.js", new HashMap<String, String>());
      assertEquals(2, results.size());
      assertEquals(3209, results.get(0).size());
      assertEquals(3209, results.get(1).size());
      assertTrue(results.get(0).get("macbeth").equals(Long.valueOf(287)));
      assertTrue(results.get(1).get("macbeth").equals(Long.valueOf(287)));
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testExecPutConstantGet(String cacheName) throws IOException {
      withScript(manager(0), "/test-put-constant-get.js", scriptName -> {
         Map<String, String> params = new HashMap<>();
         String result = clients.get(0).getCache(cacheName).execute(scriptName, params);
         assertEquals("hoptimus prime", result);
         assertEquals("hoptimus prime", clients.get(0).getCache(cacheName).get("a"));
      });
   }

   @DataProvider(name = "CacheNameProvider")
   private static Object[][] provideCacheMode() {
      return new Object[][] {{REPL_CACHE}, {DIST_CACHE}};
   }

}
