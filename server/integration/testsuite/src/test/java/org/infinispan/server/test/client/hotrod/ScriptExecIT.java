package org.infinispan.server.test.client.hotrod;

import static java.util.Collections.emptyMap;
import static org.infinispan.test.TestingUtil.loadFileAsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * @author gustavonalle
 * @since 8.0
 */
@RunWith(Arquillian.class)
@Category(HotRodSingleNode.class)
public class ScriptExecIT {

   private static final String SCRIPT_CACHE_NAME = "___script_cache";
   private static final String COMPATIBILITY_CACHE_NAME = "compatibilityCache";
   private static final String STREAM = "stream.js";

   static RemoteCacheManager remoteCacheManager;
   RemoteCache<Integer, String> remoteCache;
   RemoteCache<String, String> scriptCache;

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Rule
   public ExpectedException exceptionRule = ExpectedException.none();

   @Before
   public void initialize() {
      if (remoteCacheManager == null || !remoteCacheManager.isStarted()) {
         Configuration config = createRemoteCacheManagerConfiguration();
         remoteCacheManager = new RemoteCacheManager(config, true);
      }
      scriptCache = remoteCacheManager.getCache(SCRIPT_CACHE_NAME);
      remoteCache = remoteCacheManager.getCache();
   }

   @After
   public void clearCache() {
      remoteCache.clear();
      remoteCacheManager.getCache(COMPATIBILITY_CACHE_NAME).clear();
   }

   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   }

   private Configuration createRemoteCacheManagerConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.version(getProtocolVersion());
      config.addServer()
              .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
              .port(server1.getHotrodEndpoint().getPort());
      return config.build();
   }

   @Test
   public void testRemovingNonExistentScript() {
      exceptionRule.expect(HotRodClientException.class);
      exceptionRule.expectMessage("Unknown task");
      remoteCache.execute("nonExistent.js", new HashMap<>());
   }

   @Test
   public void testSimpleScriptExecutionWithParams() throws IOException {
      RemoteCache<String, String> remoteCache = remoteCacheManager.getCache(COMPATIBILITY_CACHE_NAME);
      addScripts("test.js");

      Map<String, Object> parameters = new HashMap<>();
      parameters.put("key", "parameter");
      parameters.put("value", "value");

      int result = remoteCache.execute("test.js", parameters);

      assertEquals(1, result);
      assertEquals("value", remoteCache.get("parameter"));
   }

   @Test
   public void testMapReduceScriptExecution() throws IOException {
      RemoteCache<String, String> remoteCache = remoteCacheManager.getCache(COMPATIBILITY_CACHE_NAME);
      addScripts("stream_serverTask.js");
      remoteCache.put("1", "word1 word2 word3");
      remoteCache.put("2", "word1 word2");
      remoteCache.put("3", "word1");

      Map<String, Long> results = remoteCache.execute("stream_serverTask.js", emptyMap());

      assertEquals(3, results.size());
      assertTrue(results.get("word1").equals(Long.valueOf(3)));
      assertTrue(results.get("word2").equals(Long.valueOf(2)));
      assertTrue(results.get("word3").equals(Long.valueOf(1)));
   }

   @Test
   public void testScriptExecution() throws Exception {
      remoteCache.clear();
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      addScripts(STREAM);

      Map<String, Long> results = remoteCache.execute(STREAM, emptyMap());

      assertEquals(3, results.size());
      assertTrue(results.get("word1").equals(Long.valueOf(3)));
      assertTrue(results.get("word2").equals(Long.valueOf(2)));
      assertTrue(results.get("word3").equals(Long.valueOf(1)));
   }

   private void addScripts(String... scripts) throws IOException {
      for (String script : scripts) {
         try (InputStream in = this.getClass().getClassLoader().getResourceAsStream(script)) {
            scriptCache.put(script, loadFileAsString(in));
         }
      }
   }

   @AfterClass
   public static void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.stop();
      }
   }

}
