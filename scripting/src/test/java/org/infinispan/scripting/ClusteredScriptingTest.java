package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.tasks.TaskContext;
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
      String s = (String) scriptingManager.runScript(scriptName, new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", s);
   }

   public void testClusteredScriptExec() throws IOException, InterruptedException, ExecutionException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      loadScript(scriptingManager, "/test.js");
      executeScriptOnManager(0, "test.js");
      executeScriptOnManager(1, "test.js");
   }

   public void testDistExecScript() throws IOException, InterruptedException, ExecutionException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      loadScript(scriptingManager, "/distExec.js");
      CompletableFuture<ArrayList<JGroupsAddress>> resultsFuture = scriptingManager.runScript("distExec.js", new TaskContext().cache(cache(0)));
      ArrayList<JGroupsAddress> results = resultsFuture.get();
      assertEquals(2, results.size());
      assertTrue(results.contains(manager(0).getAddress()));
      assertTrue(results.contains(manager(1).getAddress()));
   }

   public void testClusteredScriptStream() throws InterruptedException, ExecutionException, IOException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      Cache<String, String> cache = cache(0);
      loadData(cache, "/macbeth.txt");
      loadScript(scriptingManager, "/wordCountStream.js");
      CompletableFuture<Map<String, Long>> resultsFuture = scriptingManager.runScript("wordCountStream.js", new TaskContext().cache(cache(0)));
      Map<String, Long> results = resultsFuture.get();
      assertEquals(3209, results.size());
      assertEquals(results.get("macbeth"), Long.valueOf(287));
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
      CompletableFuture<Object> future = scriptingManager.runScript("wordCountMapper.js", new TaskContext().cache(cache));
      LinkedHashMap<String, Double> results = (LinkedHashMap<String, Double>)future.get();
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
