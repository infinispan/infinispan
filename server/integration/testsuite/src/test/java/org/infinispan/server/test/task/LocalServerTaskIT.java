package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.task.servertask.JSExecutingServerTask;
import org.infinispan.server.test.task.servertask.LocalExceptionalServerTask;
import org.infinispan.server.test.task.servertask.LocalMapReduceServerTask;
import org.infinispan.server.test.task.servertask.LocalTestServerTask;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(Arquillian.class)
@Category({Task.class})
public class LocalServerTaskIT {

   @InfinispanResource("standalone-customtask")
   RemoteInfinispanServer server;

   @Rule
   public ExpectedException exceptionRule = ExpectedException.none();

   @BeforeClass
   public static void before() throws Exception {
      String serverDir = System.getProperty("server1.dist");

      JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
      jar.addClass(LocalTestServerTask.class);
      jar.addClass(LocalExceptionalServerTask.class);
      jar.addClass(LocalMapReduceServerTask.class);
      jar.addClass(JSExecutingServerTask.class);
      jar.addAsServiceProvider(ServerTask.class, LocalTestServerTask.class,
              LocalExceptionalServerTask.class, LocalMapReduceServerTask.class, JSExecutingServerTask.class);
      jar.addAsResource(new File("/stream_serverTask.js"));
      jar.addAsManifestResource("MANIFEST.MF");

      File f = new File(serverDir, "/standalone/deployments/custom-task.jar");
      jar.as(ZipExporter.class).exportTo(f, true);
   }

   @AfterClass
   public static void undeploy() {
      String serverDir = System.getProperty("server1.dist");
      File jar = new File(serverDir, "/standalone/deployments/custom-task.jar");
      if (jar.exists())
         jar.delete();

      File f = new File(serverDir, "/standalone/deployments/custom-task.jar.deployed");
      if (f.exists())
         f.delete();
   }

   @Test
   @WithRunningServer({@RunningServer(name = "standalone-customtask")})
   public void shouldModifyCacheInViaTask() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);

      String value = "value";
      rcm.getCache().put(LocalTestServerTask.TASK_EXECUTED, value);
      rcm.getCache().execute(LocalTestServerTask.NAME, Collections.emptyMap());

      assertEquals(LocalTestServerTask.MODIFIED_PREFIX + value, rcm.getCache().get(LocalTestServerTask.TASK_EXECUTED));
      assertEquals(LocalTestServerTask.MODIFIED_PREFIX + value, rcm.getCache(LocalTestServerTask.CACHE_NAME).get(LocalTestServerTask.TASK_EXECUTED));
   }

   @Test
   @WithRunningServer({@RunningServer(name = "standalone-customtask")})
   public void shouldThrowExceptionInViaTask() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);

      exceptionRule.expect(HotRodClientException.class);
      exceptionRule.expectMessage(LocalExceptionalServerTask.EXCEPTION_MESSAGE);

      rcm.getCache().execute(LocalExceptionalServerTask.NAME, Collections.emptyMap());
   }

   @Test
   @WithRunningServer({@RunningServer(name = "standalone-customtask")})
   public void shouldExecuteMapReduceViaTask() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      RemoteCache remoteCache = rcm.getCache();
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      Map<String, Long> result = (Map<String, Long>)remoteCache.execute(LocalMapReduceServerTask.NAME, Collections.emptyMap());
      assertEquals(3, result.size());
      assertEquals(3, result.get("word1").intValue());
      assertEquals(2, result.get("word2").intValue());
      assertEquals(1, result.get("word3").intValue());
   }

   @Test
   @WithRunningServer({@RunningServer(name = "standalone-customtask")})
   public void shouldExecuteMapReduceViaJavaScriptInTask() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      RemoteCache remoteCache = rcm.getCache();
      remoteCache.put(1, "word1 word2 word3");
      remoteCache.put(2, "word1 word2");
      remoteCache.put(3, "word1");

      Map<String, Long> result = (Map<String, Long>)remoteCache.execute(JSExecutingServerTask.NAME, Collections.emptyMap());
      assertEquals(3, result.size());
      assertEquals(3, result.get("word1").intValue());
      assertEquals(2, result.get("word2").intValue());
      assertEquals(1, result.get("word3").intValue());
   }
}
