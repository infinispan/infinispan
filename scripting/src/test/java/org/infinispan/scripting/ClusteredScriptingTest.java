package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scripting.ClusteredScriptingTest")
public class ClusteredScriptingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Exception {
      final ConfigurationBuilder conf = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createCluster(conf, 2);
      waitForClusterToForm();
   }

   private void executeScriptOnManager(int num, String scriptName) throws InterruptedException, ExecutionException {
      ScriptingManager scriptingManager = getScriptingManager(manager(num));
      String s = (String) scriptingManager.runScript(scriptName).get();
      assertEquals("a", s);
   }

   public void testClusteredScriptExec() throws IOException, InterruptedException, ExecutionException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      loadScript(scriptingManager, "/test.js");
      executeScriptOnManager(0, "test.js");
      executeScriptOnManager(1, "test.js");
   }

   public void testDistExecScript() throws InterruptedException, ExecutionException, IOException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      loadScript(scriptingManager, "/distExec.js");
      Future<List<Address>> resultsFuture = scriptingManager.runScript("distExec.js", cache(0));
      List<Address> results = resultsFuture.get();
      assertEquals(2, results.size());
      Set<Address> addresses = new HashSet<>();
      for (Address result : results) {
         addresses.add(result);
      }
      assertTrue(addresses.containsAll(manager(0).getMembers()));
   }

   private ScriptingManager getScriptingManager(EmbeddedCacheManager manager) {
      return manager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
   }

   public void testMapReduce() throws Exception {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      Cache<String, String> cache = cache(0);
      loadData(cache, "/macbeth.txt");
      loadScript(scriptingManager, "/wordCountMapper.js");
      loadScript(scriptingManager, "/wordCountReducer.js");
      loadScript(scriptingManager, "/wordCountCollator.js");
      NotifyingFuture<Object> resultFuture = scriptingManager.runScript("wordCountMapper.js", cache);
      LinkedHashMap<String, Double> results = (LinkedHashMap<String, Double>)resultFuture.get();
      assertEquals(20, results.size());
      assertTrue(results.get("macbeth").equals(Double.valueOf(287)));
   }

   private void loadData(Cache<String, String> cache, String fileName) throws IOException {
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

   private void loadScript(ScriptingManager scriptingManager, String fileName) throws IOException {
      try (InputStream is = this.getClass().getResourceAsStream(fileName)) {
         String script = TestingUtil.loadFileAsString(is);
         scriptingManager.addScript(fileName.replaceAll("\\/", ""), script);
      }
   }

}
