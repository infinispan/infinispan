package org.infinispan.client.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.scripting.ScriptingManager.SCRIPT_CACHE;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for scripts with application/json data type interacting with default caches.
 *
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.ProtobufJsonScriptTest")
public class JsonScriptTest extends MultiHotRodServersTest {

   private static final String SCRIPT_NAME = "json-script.js";
   private static final int CLUSTER_SIZE = 2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(CLUSTER_SIZE, cfgBuilder);
      waitForClusterToForm();
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort).marshaller(new UTF8StringMarshaller());
   }

   @Test
   public void testJSONScript() throws IOException {
      RemoteCacheManager remoteCacheManager = client(0);
      registerScript(remoteCacheManager, SCRIPT_NAME);

      RemoteCache<String, String> cache = remoteCacheManager.getCache()
            .withDataFormat(DataFormat.builder().valueType(APPLICATION_JSON).build());

      String result = cache.execute(SCRIPT_NAME, Collections.emptyMap());

      assertEquals(result, "{\"v\":\"value2\"}");
   }

   private void registerScript(RemoteCacheManager remoteCacheManager, String script) throws IOException {
      RemoteCache<String, String> scriptCache = remoteCacheManager.getCache(SCRIPT_CACHE);
      String string = Util.getResourceAsString("/" + script, getClass().getClassLoader());
      scriptCache.put(SCRIPT_NAME, string);
   }

}
