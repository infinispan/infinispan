package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withScript;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * These tests mimic how Javascript clients remotely execute typed scripts.
 * To help bridge the gap between the Javascript client and these tests, the
 * scripts are added by remotely storing them via the script cache, and the
 * execution is done with a String marshaller.
 */
@Test(groups = "functional", testName = "client.hotrod.ExecTypedTest")
public class ExecTypedTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 2;
   static final String NAME = "exec-typed-cache";
   RemoteCacheManager execClient;
   RemoteCacheManager addScriptClient;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, new ConfigurationBuilder());
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      defineInAll(NAME, builder);
      waitForClusterToForm();
      waitForClusterToForm(SCRIPT_CACHE_NAME);
      execClient = createExecClient();
      clients.add(execClient);
      addScriptClient = createAddScriptClient();
      clients.add(addScriptClient);
   }

   protected ProtocolVersion getProtocolVersion() {
      return ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
   }

   private RemoteCacheManager createExecClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            super.createHotRodClientConfigurationBuilder(servers.get(0));
      clientBuilder.marshaller(new UTF8StringMarshaller());
      clientBuilder.version(getProtocolVersion());
      return new RemoteCacheManager(clientBuilder.build());
   }

   private RemoteCacheManager createAddScriptClient() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            super.createHotRodClientConfigurationBuilder(servers.get(0));
      clientBuilder.version(getProtocolVersion());
      return new RemoteCacheManager(clientBuilder.build());
   }

   public void testLocalTypedExecPutGet() {
      execPutGet("/typed-put-get.js", ExecMode.LOCAL, "local-typed-key", "local-typed-value");
   }

   public void testLocalTypedExecPutGetCyrillic() {
      execPutGet("/typed-put-get.js", ExecMode.LOCAL, "բարև", "привет");
   }

   public void testLocalTypedExecPutGetEmptyString() {
      withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), "/typed-put-get.js", scriptName -> {
         Map<String, String> params = new HashMap<>();
         params.put("k", "empty-key");
         params.put("v", "");
         String result = execClient.getCache(NAME).execute(scriptName, params);
         assertEquals(null, result);
      });
   }

   public void testLocalTypedExecSize() {
      withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), "/typed-size.js", scriptName -> {
         execClient.getCache(NAME).clear();
         String result = execClient.getCache(NAME).execute(scriptName, new HashMap<>());
         assertEquals("0", result);
      });
   }

   public void testLocalTypedExecWithCacheManager() {
      withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), "/typed-cachemanager-put-get.js", scriptName -> {
         String result = execClient.getCache(NAME).execute(scriptName, new HashMap<>());
         assertEquals("a", result);
      });
   }

   public void testLocalTypedExecNullReturn() {
      withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), "/typed-null-return.js", scriptName -> {
         String result = execClient.getCache(NAME).execute(scriptName, new HashMap<>());
         assertEquals(null, result);
      });
   }

   public void testDistTypedExecNullReturn() {
      withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), "/typed-dist-null-return.js", scriptName -> {
         String result = execClient.getCache(NAME).execute(scriptName, new HashMap<>());
         assertEquals("[null,null]", result.replaceAll("\\s", ""));

         String resultAsJson = execClient.getCache(NAME)
               .withDataFormat(DataFormat.builder().valueType(APPLICATION_JSON).build())
               .execute(scriptName, new HashMap<>());
         // Since the array contains nulls, Jackson cannot simplify the type
         assertEquals("[\"java.util.ArrayList\",[null,null]]", resultAsJson.replaceAll("\\s", ""));
      });
   }

   public void testDistTypedExecPutGet() {
      execPutGet("/typed-put-get-dist.js", ExecMode.DIST, "dist-typed-key", "dist-typed-value");
   }

   public void testLocalTypedExecPutGetWithListener() {
      EventLogListener<String> l = new EventLogListener<>(execClient.getCache(NAME));
      withClientListener(l, remote -> {
         withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), "/typed-put-get.js", scriptName -> {
            Map<String, String> params = new HashMap<>();
            params.put("k", "local-typed-key-listen");
            params.put("v", "local-typed-value-listen");
            String result = remote.execute(scriptName, params);
            l.expectOnlyCreatedEvent("local-typed-key-listen");
            assertEquals("local-typed-value-listen", result);
         });
      });
   }

   private void execPutGet(String path, ExecMode mode, String key, String value) {
      withScript(addScriptClient.getCache(SCRIPT_CACHE_NAME), path, scriptName -> {
         Map<String, String> params = new HashMap<>();
         params.put("k", key);
         params.put("v", value);
         String result = execClient.getCache(NAME).execute(scriptName, params);
         mode.assertResult.accept(value, result);
      });
   }

   enum ExecMode {
      LOCAL(AssertJUnit::assertEquals),
      DIST((v, r) -> assertEquals(String.format("[\"%1$s\", \"%1$s\"]", v), r));

      final BiConsumer<String, String> assertResult;

      ExecMode(BiConsumer<String, String> assertResult) {
         this.assertResult = assertResult;
      }
   }

}
