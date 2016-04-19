package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.marshall.StringMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withScript;
import static org.testng.AssertJUnit.assertEquals;

/**
 * These tests mimic how Javascript clients remotely execute typed scripts.
 * To help bridge the gap between the Javascript client and these tests, the
 * scripts are added by remotely storing them via the script cache, and the
 * execution is done with a String marshaller.
 */
@Test(groups = "functional", testName = "client.hotrod.ExecTypedTest")
public class ExecTypedTest extends MultiHotRodServersTest {

   private static final String SCRIPT_CACHE = "___script_cache";
   private static final int NUM_SERVERS = 2;
   RemoteCacheManager execClient;
   RemoteCacheManager addScriptClient;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.dataContainer()
            .keyEquivalence(AnyEquivalence.getInstance())
            .valueEquivalence(AnyEquivalence.getInstance());
      createHotRodServers(NUM_SERVERS, builder);
      execClient = createExecClient();
      addScriptClient = createAddScriptClient();
   }

   private RemoteCacheManager createExecClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            super.createHotRodClientConfigurationBuilder(servers.get(0).getPort());
      clientBuilder.marshaller(new StringMarshaller(Charset.forName("UTF-8")));
      return new InternalRemoteCacheManager(clientBuilder.build());
   }

   private RemoteCacheManager createAddScriptClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            super.createHotRodClientConfigurationBuilder(servers.get(0).getPort());
      return new InternalRemoteCacheManager(clientBuilder.build());
   }

   public void testRemoteTypedScriptPutGetExecute() throws Exception {
      withScript(addScriptClient.getCache(SCRIPT_CACHE), "/typed-put-get.js", scriptName -> {
         Map<String, String> params = new HashMap<>();
         params.put("k", "typed-key");
         params.put("v", "typed-value");
         String result = execClient.getCache().execute(scriptName, params);
         assertEquals("typed-value", result);
      });
   }

   public void testRemoteTypedScriptPutGetExecuteCyrillic() throws Exception {
      withScript(addScriptClient.getCache(SCRIPT_CACHE), "/typed-put-get.js", scriptName -> {
         Map<String, String> params = new HashMap<>();
         params.put("k", "բարև");
         params.put("v", "привет");
         String result = execClient.getCache().execute(scriptName, params);
         assertEquals("привет", result);
      });
   }

   public void testRemoteTypedScriptSizeExecute() throws Exception {
      withScript(addScriptClient.getCache(SCRIPT_CACHE), "/typed-size.js", scriptName -> {
         clients.get(0).getCache().clear();
         String result = execClient.getCache().execute(scriptName, new HashMap<>());
         assertEquals("0", result);
      });
   }

   public void testRemoteTypedCacheManagerScriptExecute() throws Exception {
      withScript(addScriptClient.getCache(SCRIPT_CACHE), "/typed-cachemanager-put-get.js", scriptName -> {
         String result = execClient.getCache().execute(scriptName, new HashMap<>());
         assertEquals("a", result);
      });
   }

}
