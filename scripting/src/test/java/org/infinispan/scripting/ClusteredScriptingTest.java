package org.infinispan.scripting;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.infinispan.scripting.utils.ScriptingUtils.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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

   @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*without a cache binding.*")
   public void testDistributedScriptExecutionWithoutCacheBinding() throws IOException, ExecutionException, InterruptedException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      loadScript(scriptingManager, "/distExec.js");

      scriptingManager.runScript("distExec.js").get();
   }

   @Test(enabled = false, description = "Disabled due to ISPN-6173.")
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

   @Test(enabled = false, description = "Disabled due to ISPN-6173.")
   public void testDistributedMapReduceStream() throws IOException, ExecutionException, InterruptedException {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      Cache cache = cache(0);

      loadData(cache, "/macbeth.txt");
      loadScript(scriptingManager, "/wordCountStream_dist.js");

      ArrayList<Map<String, Long>> resultsFuture =  (ArrayList<Map<String, Long>>) scriptingManager.runScript("wordCountStream_dist.js", new TaskContext().cache(cache(0))).get();
      assertEquals(2, resultsFuture.size());
      assertEquals(3209, resultsFuture.get(0).size());
      assertEquals(3209, resultsFuture.get(1).size());
      assertEquals(resultsFuture.get(0).get("macbeth"), Long.valueOf(287));
      assertEquals(resultsFuture.get(1).get("macbeth"), Long.valueOf(287));
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

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*cannot be invoked directly since it specifies mode 'REDUCER'")
   public void testOnlyReduceTaskRun() throws Exception {
      ScriptingManager scriptingManager = getScriptingManager(manager(0));
      Cache<String, String> cache = cache(0);
      loadData(cache, "/macbeth.txt");
      loadScript(scriptingManager, "/wordCountReducer.js");

      scriptingManager.runScript("wordCountReducer.js", new TaskContext().cache(cache));
   }
}
