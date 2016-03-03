package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "client.hotrod.ExecTypedTest")
public class ExecTypedTest extends MultiHotRodServersTest {

   private static final String SCRIPT_CACHE = "___script_cache";
   private static final int NUM_SERVERS = 2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.dataContainer()
            .keyEquivalence(AnyEquivalence.getInstance())
            .valueEquivalence(AnyEquivalence.getInstance());
      createHotRodServers(NUM_SERVERS, builder);
   }

   public void testRemoteTypedScriptPutGetExecute() throws Exception {
      loadScript("testRemoteTypedScriptPutGetExecute.js", "/typed-put-get.js");
      Map<String, String> params = new HashMap<>();
      params.put("k", "typed-key");
      params.put("v", "typed-value");
      String result = clients.get(0).getCache().execute("testRemoteTypedScriptPutGetExecute.js", params);
      assertEquals("typed-value", result);
   }

   private void loadScript(String name, String scriptName) throws IOException {
      try (InputStream is = this.getClass().getResourceAsStream(scriptName)) {
         String script = TestingUtil.loadFileAsString(is);
         ScriptingManager scriptingManager = manager(0)
               .getGlobalComponentRegistry().getComponent(ScriptingManager.class);
         scriptingManager.addScript(name, script);
      }
   }

}
