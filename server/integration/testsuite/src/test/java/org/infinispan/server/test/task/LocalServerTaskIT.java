package org.infinispan.server.test.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.task.servertask.Greeting;
import org.infinispan.server.test.task.servertask.GreetingServerTask;
import org.infinispan.server.test.task.servertask.JSExecutingServerTask;
import org.infinispan.server.test.task.servertask.LocalExceptionalServerTask;
import org.infinispan.server.test.task.servertask.LocalMapReduceServerTask;
import org.infinispan.server.test.task.servertask.LocalTestServerTask;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.ManagementClient;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({SingleNode.class})
public class LocalServerTaskIT {

   private static final String CACHE_CONTAINER = "local";
   private static final String CACHE_TEMPLATE = "localCacheConfiguration";
   private RemoteCache cache;

   @InfinispanResource("container1")
   RemoteInfinispanServer server;

   @Rule
   public ExpectedException exceptionRule = ExpectedException.none();

   @Before
   public void before() throws Exception {
      server.reconnect();
      if (cache == null) {
         cache = ITestUtils.createCacheManager(server).getCache(LocalTestServerTask.CACHE_NAME);
      }
      cache.clear();
   }

   @BeforeClass
   public static void beforeClass() throws Exception {
      String serverDir = System.getProperty("server1.dist");
      JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
      jar.addClass(LocalTestServerTask.class);
      jar.addClass(LocalExceptionalServerTask.class);
      jar.addClass(LocalMapReduceServerTask.class);
      jar.addClass(JSExecutingServerTask.class);
      jar.addClass(GreetingServerTask.class);
      jar.addClass(Greeting.class);
      jar.addAsServiceProvider(ServerTask.class, LocalTestServerTask.class,
              LocalExceptionalServerTask.class, LocalMapReduceServerTask.class, JSExecutingServerTask.class, GreetingServerTask.class);
      jar.addAsResource(new File("/stream_serverTask.js"));
      jar.addAsManifestResource("MANIFEST.MF");

      File f = new File(serverDir, "/standalone/deployments/custom-task.jar");
      jar.as(ZipExporter.class).exportTo(f, true);

      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.enableCompatibilityForConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.addCache(LocalTestServerTask.CACHE_NAME, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
      client.reload();
   }

   @AfterClass
   public static void afterClass() throws Exception {
      String serverDir = System.getProperty("server1.dist");
      File jar = new File(serverDir, "/standalone/deployments/custom-task.jar");
      if (jar.exists())
         jar.delete();

      File f = new File(serverDir, "/standalone/deployments/custom-task.jar.deployed");
      if (f.exists())
         f.delete();

      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.removeCache(LocalTestServerTask.CACHE_NAME, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
      client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
   }

   @Test
   public void shouldModifyCacheInViaTask() throws Exception {
      String value = "value";
      cache.put(LocalTestServerTask.TASK_EXECUTED, value);
      cache.execute(LocalTestServerTask.NAME, Collections.emptyMap());

      assertEquals(LocalTestServerTask.MODIFIED_PREFIX + value, cache.get(LocalTestServerTask.TASK_EXECUTED));
   }

   @Test
   public void shouldThrowExceptionInViaTask() throws Exception {
      exceptionRule.expect(HotRodClientException.class);
      exceptionRule.expectMessage(LocalExceptionalServerTask.EXCEPTION_MESSAGE);
      cache.execute(LocalExceptionalServerTask.NAME, Collections.emptyMap());
   }

   @Test
   public void shouldExecuteMapReduceViaTask() throws Exception {
      cache.put(1, "word1 word2 word3");
      cache.put(2, "word1 word2");
      cache.put(3, "word1");

      Map<String, Long> result = (Map<String, Long>)cache.execute(LocalMapReduceServerTask.NAME, Collections.emptyMap());
      assertEquals(3, result.size());
      assertEquals(3, result.get("word1").intValue());
      assertEquals(2, result.get("word2").intValue());
      assertEquals(1, result.get("word3").intValue());
   }

   @Test
   public void shouldExecuteMapReduceViaJavaScriptInTask() throws Exception {
      cache.put(1, "word1 word2 word3");
      cache.put(2, "word1 word2");
      cache.put(3, "word1");

      Map<String, Long> result = (Map<String, Long>)cache.execute(JSExecutingServerTask.NAME, Collections.emptyMap());
      assertEquals(3, result.size());
      assertEquals(3, result.get("word1").intValue());
      assertEquals(2, result.get("word2").intValue());
      assertEquals(1, result.get("word3").intValue());
   }

   @Test
   public void shouldWorkWithCustomMojo() throws Exception {
      Map params = new HashMap();
      params.put("greeting", toBytes(new Greeting("hello, good morning :)")));

      String result = (String) cache.execute(GreetingServerTask.NAME, params);
      assertEquals("hello, good morning :)", result);
   }

   private byte[] toBytes(Object o) {
      try {
         ByteArrayOutputStream os = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(os);
         oos.writeObject(o);
         oos.close();
         return os.toByteArray();
      } catch (IOException e) {
         throw new AssertionError(e);
      }
   }
}
