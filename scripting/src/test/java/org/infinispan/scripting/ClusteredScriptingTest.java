package org.infinispan.scripting;

import static org.infinispan.scripting.utils.ScriptingUtils.getScriptingManager;
import static org.infinispan.scripting.utils.ScriptingUtils.loadData;
import static org.infinispan.scripting.utils.ScriptingUtils.loadScript;
import static org.infinispan.test.TestingUtil.waitForStableTopology;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scripting.ClusteredScriptingTest")
public class ClusteredScriptingTest extends AbstractInfinispanTest {

   public static final int EXPECTED_WORDS = 3202;

   @Test(dataProvider = "cacheModeProvider")
   public void testLocalScriptExecutionWithCache(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(
              TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         @Override
         public void call() throws IOException, ExecutionException, InterruptedException {
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            for(EmbeddedCacheManager cm : cms) {
               cm.defineConfiguration(ScriptingTest.CACHE_NAME, cm.getDefaultCacheConfiguration());
            }
            Cache cache = cms[0].getCache(ScriptingTest.CACHE_NAME);
            loadScript(scriptingManager, "/test.js");
            executeScriptOnManager("test.js", cms[0]);
            executeScriptOnManager("test.js", cms[1]);
         }
      });
   }

   @Test(dataProvider = "cacheModeProvider")
   public void testLocalScriptExecutionWithCache1(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {

         @Override
         public void call() throws Exception {
            for(EmbeddedCacheManager cm : cms) {
               cm.defineConfiguration(ScriptingTest.CACHE_NAME, cm.getDefaultCacheConfiguration());
            }
            Cache<Object, Object> cache = cms[0].getCache(ScriptingTest.CACHE_NAME);
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            loadScript(scriptingManager, "/test1.js");

            cache.put("a", "newValue");

            executeScriptOnManager("test1.js", cms[0]);
            executeScriptOnManager("test1.js", cms[1]);
         }
      });
   }

   @Test(dataProvider = "cacheModeProvider")
   public void testDistExecScriptWithCache(final CacheMode cacheMode) throws IOException, InterruptedException, ExecutionException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            Cache cache1 = cms[0].getCache();
            Cache cache2 = cms[1].getCache();
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            loadScript(scriptingManager, "/distExec1.js");
            waitForStableTopology(cache1, cache2);

            CompletableFuture<ArrayList<JGroupsAddress>> resultsFuture = scriptingManager.runScript("distExec1.js", new TaskContext().cache(cache1));
            ArrayList<JGroupsAddress> results = resultsFuture.get();
            assertEquals(2, results.size());

            assertTrue(results.contains(cms[0].getAddress()));
            assertTrue(results.contains(cms[1].getAddress()));
         }
      });
   }

   @Test(dataProvider = "cacheModeProvider")
   public void testDistExecScriptWithCacheManagerAndParams(final CacheMode cacheMode) throws IOException, InterruptedException, ExecutionException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            Cache cache1 = cms[0].getCache();
            Cache cache2 = cms[1].getCache();

            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            loadScript(scriptingManager, "/distExec.js");
            waitForStableTopology(cache1, cache2);

            CompletableFuture<ArrayList<JGroupsAddress>> resultsFuture = scriptingManager.runScript("distExec.js",
                    new TaskContext().cache(cache1).addParameter("a", "value"));

            ArrayList<JGroupsAddress> results = resultsFuture.get();
            assertEquals(2, results.size());
            assertTrue(results.contains(cms[0].getAddress()));
            assertTrue(results.contains(cms[1].getAddress()));

            assertEquals("value", cache1.get("a"));
            assertEquals("value", cache2.get("a"));
         }
      });
   }

   @Test(expectedExceptions = IllegalStateException.class, dataProvider = "cacheModeProvider", expectedExceptionsMessageRegExp = ".*without a cache binding.*")
   public void testDistributedScriptExecutionWithoutCacheBinding(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            loadScript(scriptingManager, "/distExec.js");

            scriptingManager.runScript("distExec.js").get();
         }
      });
   }

   @Test(dataProvider = "cacheModeProvider")
   public void testDistributedMapReduceStreamWithFlag(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            Cache cache1 = cms[0].getCache();
            Cache cache2 = cms[1].getCache();

            loadData(cache1, "/macbeth.txt");
            loadScript(scriptingManager, "/wordCountStream.js");
            waitForStableTopology(cache1, cache2);

            Map<String, Long> resultsFuture = (Map<String, Long>) scriptingManager.runScript(
                    "wordCountStream.js", new TaskContext().cache(cache1.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL))).get();
            assertEquals(EXPECTED_WORDS, resultsFuture.size());
            assertEquals(resultsFuture.get("macbeth"), Long.valueOf(287));

            resultsFuture = (Map<String, Long>) scriptingManager.runScript(
                    "wordCountStream.js", new TaskContext().cache(cache1.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL))).get();
            assertEquals(EXPECTED_WORDS, resultsFuture.size());
            assertEquals(resultsFuture.get("macbeth"), Long.valueOf(287));
         }
      });
   }


   @Test(enabled = false, dataProvider = "cacheModeProvider", description = "Disabled due to ISPN-6173.")
   public void testDistributedMapReduceStreamLocalMode(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            Cache cache1 = cms[0].getCache();
            Cache cache2 = cms[1].getCache();

            loadData(cache1, "/macbeth.txt");
            loadScript(scriptingManager, "/wordCountStream_serializable.js");
            waitForStableTopology(cache1, cache2);

            ArrayList<Map<String, Long>> resultsFuture = (ArrayList<Map<String, Long>>) scriptingManager.runScript(
                    "wordCountStream_serializable.js", new TaskContext().cache(cache1)).get();
            assertEquals(2, resultsFuture.size());
            assertEquals(EXPECTED_WORDS, resultsFuture.get(0).size());
            assertEquals(EXPECTED_WORDS, resultsFuture.get(1).size());
            assertEquals(resultsFuture.get(0).get("macbeth"), Long.valueOf(287));
            assertEquals(resultsFuture.get(1).get("macbeth"), Long.valueOf(287));
         }
      });
   }

   @Test(enabled = false, dataProvider = "cacheModeProvider", description = "Disabled due to ISPN-6173.")
   public void testDistributedMapReduceStreamLocalModeWithExecutors(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            Cache cache1 = cms[0].getCache();
            Cache cache2 = cms[1].getCache();

            loadData(cache1, "/macbeth.txt");
            loadScript(scriptingManager, "/wordCountStream_Exec.js");
            waitForStableTopology(cache1, cache2);

            ArrayList<Map<String, Long>> resultsFuture = (ArrayList<Map<String, Long>>) scriptingManager.runScript(
                    "wordCountStream_Exec.js", new TaskContext().cache(cache1)).get();
            assertEquals(2, resultsFuture.size());
            assertEquals(EXPECTED_WORDS, resultsFuture.get(0).size());
            assertEquals(EXPECTED_WORDS, resultsFuture.get(1).size());
            assertEquals(resultsFuture.get(0).get("macbeth"), Long.valueOf(287));
            assertEquals(resultsFuture.get(1).get("macbeth"), Long.valueOf(287));
         }
      });
   }

   @Test(enabled = false, dataProvider = "cacheModeProvider", description = "Disabled due to ISPN-6173.")
   public void testDistributedMapReduceStream(final CacheMode cacheMode) throws IOException, ExecutionException, InterruptedException {
      withCacheManagers(new MultiCacheManagerCallable(TestCacheManagerFactory.createCacheManager(cacheMode, false),
              TestCacheManagerFactory.createCacheManager(cacheMode, false)) {
         public void call() throws Exception {
            ScriptingManager scriptingManager = getScriptingManager(cms[0]);
            Cache cache1 = cms[0].getCache();
            Cache cache2 = cms[1].getCache();

            loadData(cache1, "/macbeth.txt");
            loadScript(scriptingManager, "/wordCountStream_dist.js");
            waitForStableTopology(cache1, cache2);

            ArrayList<Map<String, Long>> resultsFuture = (ArrayList<Map<String, Long>>) scriptingManager.runScript(
                    "wordCountStream_dist.js", new TaskContext().cache(cache1)).get();
            assertEquals(2, resultsFuture.size());
            assertEquals(EXPECTED_WORDS, resultsFuture.get(0).size());
            assertEquals(EXPECTED_WORDS, resultsFuture.get(1).size());
            assertEquals(resultsFuture.get(0).get("macbeth"), Long.valueOf(287));
            assertEquals(resultsFuture.get(1).get("macbeth"), Long.valueOf(287));
         }
      });
   }

   private void executeScriptOnManager(String scriptName, EmbeddedCacheManager cacheManager) throws InterruptedException, ExecutionException {
      ScriptingManager scriptingManager = getScriptingManager(cacheManager);
      String value = (String) scriptingManager.runScript(scriptName, new TaskContext().addParameter("a", "value")).get();
      assertEquals(value, cacheManager.getCache(ScriptingTest.CACHE_NAME).get("a"));
   }

   @DataProvider(name = "cacheModeProvider")
   private static Object[][] provideCacheMode() {
      return new Object[][] {{CacheMode.REPL_SYNC}, {CacheMode.DIST_SYNC}};
   }
}
