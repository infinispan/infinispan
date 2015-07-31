package org.infinispan.server.test.client.hotrod;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.category.HotRodLocal;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.infinispan.test.TestingUtil.loadFileAsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author gustavonalle
 * @since 8.0
 */
@RunWith(Arquillian.class)
@Category(HotRodLocal.class)
public class ScriptExecIT {

   private static final String SCRIPT_CACHE_NAME = "___script_cache";
   private static final String MAPPER = "mapper.js";
   private static final String REDUCER = "reducer.js";
   private static final String COLLATOR = "collator.js";

   static RemoteCacheManager remoteCacheManager;
   RemoteCache<Integer, String> remoteCache;
   RemoteCache<String, String> scriptCache;

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Before
   public void initialize() {
      if (remoteCacheManager == null) {
         Configuration config = createRemoteCacheManagerConfiguration();
         remoteCacheManager = new RemoteCacheManager(config, true);
      }
      scriptCache = remoteCacheManager.getCache(SCRIPT_CACHE_NAME);
      remoteCache = remoteCacheManager.getCache();
   }

   private Configuration createRemoteCacheManagerConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer()
              .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
              .port(server1.getHotrodEndpoint().getPort());
      return config.build();
   }

   @Test
   public void testScriptExecution() throws Exception {
      remoteCache.clear();
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      addScripts(MAPPER, REDUCER, COLLATOR);

      Map<String, Double> results = remoteCache.execute(MAPPER, emptyMap());

      assertEquals(3, results.size());
      assertTrue(results.get("word1").equals(Double.valueOf(3)));
      assertTrue(results.get("word2").equals(Double.valueOf(2)));
      assertTrue(results.get("word3").equals(Double.valueOf(1)));
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
