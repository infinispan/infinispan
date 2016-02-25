package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.task.servertask.DistributedCacheUsingTask;
import org.infinispan.server.test.task.servertask.DistributedJSExecutingServerTask;
import org.infinispan.server.test.task.servertask.DistributedMapReduceServerTask;
import org.infinispan.server.test.task.servertask.DistributedTestServerTask;
import org.infinispan.server.test.task.servertask.JSExecutingServerTask;
import org.infinispan.server.test.task.servertask.LocalMapReduceServerTask;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@RunWith(Arquillian.class)
@Category({Task.class})
@WithRunningServer({@RunningServer(name="clusteredcache-1"), @RunningServer(name = "clusteredcache-2")})
public class DistributedServerTaskIT {

   @InfinispanResource("clusteredcache-1")
   RemoteInfinispanServer server1;

   @InfinispanResource("clusteredcache-2")
   RemoteInfinispanServer server2;

   RemoteCacheManager rcm1;
   RemoteCacheManager rcm2;

   @Rule
   public ExpectedException exceptionRule = ExpectedException.none();

   @BeforeClass
   public static void before() throws Exception {
      String[] serverDirs = new String[]{System.getProperty("server1.dist"), System.getProperty("server2.dist")};

      JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
      jar.addClass(DistributedTestServerTask.class);
      jar.addClass(DistributedCacheUsingTask.class);
      jar.addClass(DistributedMapReduceServerTask.class);
      jar.addClass(DistributedJSExecutingServerTask.class);
      jar.addClass(LocalMapReduceServerTask.class);
      jar.addClass(JSExecutingServerTask.class);
      jar.addAsServiceProvider(ServerTask.class, DistributedTestServerTask.class, DistributedCacheUsingTask.class,
              DistributedMapReduceServerTask.class, DistributedJSExecutingServerTask.class);
      jar.addAsResource(new File("/stream_serverTask.js"));
      jar.addAsManifestResource("MANIFEST.MF");

      for (String serverDir : serverDirs) {
         File f = new File(serverDir, "/standalone/deployments/custom-distributed-task.jar");
         f.deleteOnExit();
         jar.as(ZipExporter.class).exportTo(f, true);
      }
   }

   @Before
   public void setUp() {
      if (rcm1 == null) {
         Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
                 .port(server1.getHotrodEndpoint().getPort()).build();
         rcm1 = new RemoteCacheManager(conf);
      }

      if (rcm2 == null) {
         Configuration conf = new ConfigurationBuilder().addServer().host(server2.getHotrodEndpoint().getInetAddress().getHostName())
                 .port(server2.getHotrodEndpoint().getPort()).build();
         rcm2 = new RemoteCacheManager(conf);
      }
   }

   @After
   public void tearDown() {
      rcm1.getCache().clear();
      rcm1.getCache(DistributedCacheUsingTask.CACHE_NAME).clear();
      rcm1.getCache(DistributedJSExecutingServerTask.DIST_CACHE_NAME).clear();
   }

   @Test
   @SuppressWarnings("unchecked")
   public void shouldGatherNodeNamesInRemoteTasks() throws Exception {
      Object resultObject = rcm1.getCache().execute(DistributedTestServerTask.NAME, Collections.emptyMap());
      assertNotNull(resultObject);
      List<String> result = (List<String>) resultObject;
      assertEquals(2, result.size());

      assertTrue("result list does not contain expected items.", result.containsAll(asList("node0", "node1")));
   }

   @Test
   @SuppressWarnings("unchecked")
   public void shouldThrowExceptionInRemoteTasks() throws Exception {
      Map<String, Boolean> params = new HashMap<String, Boolean>();
      params.put("throwException", true);

      exceptionRule.expect(HotRodClientException.class);
      exceptionRule.expectMessage("CancellationException");

      rcm1.getCache().execute(DistributedTestServerTask.NAME, params);
   }

   @Test
   @SuppressWarnings("unchecked")
   public void shouldPutNewValueInRemoteCache() throws Exception {
      String key = "key";
      String value = "value";
      String paramValue = "parameter";
      String modifiedValue = "modified:value";

      Map<String, String> params = new HashMap<>();
      params.put(DistributedCacheUsingTask.PARAM_KEY, paramValue);
      rcm2.getCache(DistributedCacheUsingTask.CACHE_NAME);
      rcm1.getCache(DistributedCacheUsingTask.CACHE_NAME).put(key, value);

      rcm1.getCache(DistributedCacheUsingTask.CACHE_NAME).execute(DistributedCacheUsingTask.NAME, params);
      assertEquals(modifiedValue + ":" + paramValue, rcm1.getCache(DistributedCacheUsingTask.CACHE_NAME).get(key));
   }

   @Test
   @SuppressWarnings("unchecked")
   public void shouldExecuteMapReduceOnReplCacheViaTask() throws Exception {
      RemoteCache remoteCache = rcm2.getCache(DistributedMapReduceServerTask.CACHE_NAME);
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      List<Map<String, Long>> result = (List<Map<String, Long>>)remoteCache.execute(DistributedMapReduceServerTask.NAME, Collections.emptyMap());
      assertEquals(2, result.size());
      verifyMapReduceResult(result.get(0));
      verifyMapReduceResult(result.get(1));

   }

   @Test
   @SuppressWarnings("unchecked")
   public void shouldExecuteMapReduceOnDistCacheViaTask() throws Exception {
      RemoteCache remoteCache = rcm1.getCache(DistributedMapReduceServerTask.DIST_CACHE_NAME);
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      List<Map<String, Long>> result = (List<Map<String, Long>>)remoteCache.execute(DistributedMapReduceServerTask.NAME, Collections.emptyMap());
      assertEquals(2, result.size());
      verifyMapReduceResult(result.get(0));
      verifyMapReduceResult(result.get(1));
   }

   @Test
   @Ignore(value="Is disabled until the issue ISPN-6303 is fixed.")
   public void shouldExecuteMapReduceViaJavaScriptInTask() throws Exception {
      RemoteCache remoteCache = rcm2.getCache(DistributedJSExecutingServerTask.CACHE_NAME);
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      List<Map<String, Long>> result = (List<Map<String, Long>>)remoteCache.execute(DistributedJSExecutingServerTask.NAME, Collections.emptyMap());
      assertEquals(2, result.size());
      verifyMapReduceResult(result.get(0));
      verifyMapReduceResult(result.get(1));
   }

   @Test
   @Ignore(value="Is disabled until the issue ISPN-6303 and ISPN-6173 are fixed.")
   public void shouldExecuteMapReduceViaJavaScriptInTaskDistCache() throws Exception {
      RemoteCache remoteCache = rcm2.getCache(DistributedJSExecutingServerTask.DIST_CACHE_NAME);
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      Map<String, String> parameters = new HashMap<>();
      parameters.put(JSExecutingServerTask.CACHE_NAME_PARAMETER, DistributedJSExecutingServerTask.DIST_CACHE_NAME);
      List<Map<String, Long>> result = (List<Map<String, Long>>)remoteCache.execute(DistributedJSExecutingServerTask.NAME, parameters);
      assertEquals(2, result.size());
      verifyMapReduceResult(result.get(0));
      verifyMapReduceResult(result.get(1));
   }

   private void verifyMapReduceResult(Map<String, Long> result) {
      assertEquals(3, result.size());
      assertEquals(3, result.get("word1").intValue());
      assertEquals(2, result.get("word2").intValue());
      assertEquals(1, result.get("word3").intValue());
   }
}
