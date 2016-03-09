package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

   static final int NUM_SERVERS = 2;
   static final int SIZE = 20;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unknown task 'nonExistent\\.js'.*")
   public void testRemovingNonExistentScript() {
      clients.get(0).getCache().execute("nonExistent.js", new HashMap<>());
   }

   @Test(dataProvider = "CacheModeProvider")
   public void testEmbeddedScriptRemoteExecution(CacheMode cacheMode) throws IOException {
      String cacheName = "testEmbeddedScriptRemoteExecution_" + cacheMode.toString();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence()).valueEquivalence(new AnyServerEquivalence()).compatibility().enable().marshaller(new GenericJBossMarshaller());
      defineInAll(cacheName, builder);
      ScriptingManager scriptingManager = manager(0).getGlobalComponentRegistry().getComponent(ScriptingManager.class);

      try (InputStream is = this.getClass().getResourceAsStream("/test.js")) {
         String script = TestingUtil.loadFileAsString(is);
         scriptingManager.addScript("testEmbeddedScriptRemoteExecution.js", script);
      }
      populateCache(cacheName);

      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
      Map<String, String> params = new HashMap<>();
      params.put("parameter", "guinness");
      Integer result = clients.get(0).getCache(cacheName).execute("testEmbeddedScriptRemoteExecution.js", params);
      assertEquals(SIZE + 1, result.intValue());
      assertEquals("guinness", clients.get(0).getCache(cacheName).get("parameter"));
   }

   @Test(dataProvider = "CacheModeProvider")
   public void testRemoteScriptRemoteExecution(CacheMode cacheMode) throws IOException {
      String cacheName = "testRemoteScriptRemoteExecution_" + cacheMode.toString();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence()).valueEquivalence(new AnyServerEquivalence()).compatibility().enable().marshaller(new GenericJBossMarshaller());
      defineInAll(cacheName, builder);
      try (InputStream is = this.getClass().getResourceAsStream("/test.js")) {
         String script = TestingUtil.loadFileAsString(is);
         clients.get(0).getCache(SCRIPT_CACHE).put("testRemoteScriptRemoteExecution.js", script);
      }
      populateCache(cacheName);

      assertEquals(SIZE, clients.get(0).getCache(cacheName).size());
      Map<String, String> params = new HashMap<>();
      params.put("parameter", "hoptimus prime");

      Integer result = clients.get(0).getCache(cacheName).execute("testRemoteScriptRemoteExecution.js", params);
      assertEquals(SIZE + 1, result.intValue());
      assertEquals("hoptimus prime", clients.get(0).getCache(cacheName).get("parameter"));
   }

   @Test(enabled = false, dataProvider = "CacheModeProvider", description = "Enable when ISPN-6300 is fixed.")
   public void testScriptExecutionWithPassingParams(CacheMode cacheMode) throws IOException {
      String cacheName = "testScriptExecutionWithPassingParams_" + cacheMode.toString();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence()).valueEquivalence(new AnyServerEquivalence()).compatibility().enable().marshaller(new GenericJBossMarshaller());
      defineInAll(cacheName, builder);
      try (InputStream is = this.getClass().getResourceAsStream("/distExec.js")) {
         String script = TestingUtil.loadFileAsString(is);
         clients.get(0).getCache(SCRIPT_CACHE).put("testScriptExecutionWithPassingParams.js", script);
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
      loadData(cache, "/macbeth.txt");
      loadScript(scriptCache, "/wordCountStream_dist.js");

      ArrayList<Map<String, Long>> results = cache.execute("wordCountStream_dist.js", new HashMap<String, String>());
      assertEquals(2, results.size());
      assertEquals(3209, results.get(0).size());
      assertEquals(3209, results.get(1).size());
      assertTrue(results.get(0).get("macbeth").equals(Long.valueOf(287)));
      assertTrue(results.get(1).get("macbeth").equals(Long.valueOf(287)));
   }

   @DataProvider(name = "CacheModeProvider")
   private static Object[][] provideCacheMode() {
      return new Object[][] {{CacheMode.REPL_SYNC}, {CacheMode.DIST_SYNC}};
   }

   private void loadData(BasicCache<String, String> cache, String fileName) throws IOException {
      try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(fileName)))) {
         int chunkSize = 10;
         int chunkId = 0;

         CharBuffer cbuf = CharBuffer.allocate(1024 * chunkSize);
         while (bufferedReader.read(cbuf) >= 0) {
            Buffer buffer = cbuf.flip();
            String textChunk = buffer.toString();
            cache.put(fileName + (chunkId++), textChunk);
            cbuf.clear();
         }
      }
   }

   private void loadScript(BasicCache<String, String> scriptCache, String fileName) throws IOException {
      try (InputStream is = this.getClass().getResourceAsStream(fileName)) {
         String script = TestingUtil.loadFileAsString(is);
         scriptCache.put(fileName.replaceAll("\\/", ""), script);
      }
   }

}
