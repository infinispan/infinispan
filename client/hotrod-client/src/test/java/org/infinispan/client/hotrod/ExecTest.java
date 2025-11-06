package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.loadScript;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withScript;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.test.CommonsTestingUtil.loadFileAsString;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.utils.ScriptingUtils;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * ExecTest.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@Test(groups = {"functional", "smoke"}, testName = "client.hotrod.ExecTest")
public class ExecTest extends MultiHotRodServersTest {
   static final String REPL_CACHE = "R";
   static final String DIST_CACHE = "D";

   static final int NUM_SERVERS = 2;
   static final int SIZE = 20;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());
      defineInAll(REPL_CACHE, CacheMode.REPL_SYNC);
      defineInAll(DIST_CACHE, CacheMode.DIST_SYNC);
      waitForClusterToForm();
      waitForClusterToForm(SCRIPT_CACHE_NAME);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(HotRodServer server) {
      // Remote scripting must use the JavaSerializationMarshaller for now due to IPROTO-118
      return createHotRodClientConfigurationBuilder(server.getHost(), server.getPort()).marshaller(JavaSerializationMarshaller.class).addJavaSerialAllowList("java.*");
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      clients.get(0).getCache(REPL_CACHE).clear();
      clients.get(0).getCache(DIST_CACHE).clear();
   }

   private void defineInAll(String cacheName, CacheMode mode) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(mode, true);
      builder.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(APPLICATION_OBJECT_TYPE)
            .locking().isolationLevel(IsolationLevel.READ_COMMITTED);
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
      builder.encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      defineInAll(cacheName, builder);
      try (InputStream is = this.getClass().getResourceAsStream("/distExec.js")) {
         String script = loadFileAsString(is);
         manager(0).getCache(SCRIPT_CACHE_NAME).put("testScriptExecutionWithPassingParams.js", script);
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
      builder.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);
      defineInAll(cacheName, builder);
      waitForClusterToForm(cacheName);

      RemoteCache<String, String> cache = clients.get(0).getCache(cacheName);
      ScriptingUtils.loadData(cache, "/macbeth.txt");
      ScriptingManager scriptingManager = GlobalComponentRegistry.componentOf(manager(0), ScriptingManager.class);
      loadScript("/wordCountStream_dist.js", scriptingManager, "wordCountStream_dist.js");

      ArrayList<Map<String, Long>> results = cache.execute("wordCountStream_dist.js", new HashMap<String, String>());
      assertEquals(2, results.size());
      assertEquals(3202, results.get(0).size());
      assertEquals(3202, results.get(1).size());
      assertTrue(results.get(0).get("macbeth").equals(287L));
      assertTrue(results.get(1).get("macbeth").equals(287L));
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

   @Test(dataProvider = "CacheNameProvider")
   public void testExecReturnNull(String cacheName) throws IOException {
      withScript(manager(0), "/test-null-return.js", scriptName -> {
         Object result = clients.get(0).getCache(cacheName).execute(scriptName, new HashMap<>());
         assertEquals(null, result);
      });
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testLocalExecPutGet(String cacheName) {
      execPutGet(cacheName, "/test-put-get.js", ExecMode.LOCAL, "local-key", "local-value");
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testDistExecPutGet(String cacheName) {
      execPutGet(cacheName, "/test-put-get-dist.js", ExecMode.DIST, "dist-key", "dist-value");
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testLocalExecPutGetWithListener(String cacheName) {
      final EventLogListener<String> l = new EventLogListener<>(clients.get(0).getCache(cacheName));
      withClientListener(l, remote ->
            withScript(manager(0), "/test-put-get.js", scriptName -> {
               Map<String, String> params = new HashMap<>();
               params.put("k", "local-key-listen");
               params.put("v", "local-value-listen");
               String result = remote.execute(scriptName, params);
               l.expectOnlyCreatedEvent("local-key-listen");
               assertEquals("local-value-listen", result);
            }));
   }

   @Test(dataProvider = "CacheNameProvider")
   public void testExecWithHint(String cacheName) {
      withScript(manager(0), "/test-is-primary-owner.js", scriptName -> {
         RemoteCache<Object, Object> cache = clients.get(0).getCache(cacheName);
         for (int i = 0; i < 50; ++i) {
            String someKey = "someKey" + i;
            assertTrue(cache.execute(scriptName, Collections.singletonMap("k", someKey), someKey));
         }
         int notHinted = 0;
         for (int i = 0; i < 50; ++i) {
            String someKey = "someKey" + i;
            if (cache.execute(scriptName, Collections.singletonMap("k", someKey))) {
               ++notHinted;
            }
         }
         assertTrue("Not hinted: " + notHinted, notHinted > 0);
         assertTrue("Not hinted: " + notHinted, notHinted < 50);
      });
   }

   private void execPutGet(String cacheName, String path, ExecMode mode, String key, String value) {
      withScript(manager(0), path, scriptName -> {
         Map<String, String> params = new HashMap<>();
         params.put("k", key);
         params.put("v", value);
         params.put("cacheName", cacheName);
         Object results = clients.get(0).getCache(cacheName).execute(scriptName, params);
         mode.assertResult.accept(value, results);
      });
   }

   @DataProvider(name = "CacheNameProvider")
   private static Object[][] provideCacheMode() {
      return new Object[][]{{REPL_CACHE}, {DIST_CACHE}};
   }

   enum ExecMode {
      LOCAL(AssertJUnit::assertEquals),
      DIST((v, r) -> assertEquals(Arrays.asList(v, v), r));

      final BiConsumer<String, Object> assertResult;

      ExecMode(BiConsumer<String, Object> assertResult) {
         this.assertResult = assertResult;
      }
   }

}
