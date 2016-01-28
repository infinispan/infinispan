package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(Arquillian.class)
@Category({Task.class})
public class LocalServerTaskIT {

   @InfinispanResource("standalone-customtask")
   RemoteInfinispanServer server;

   @BeforeClass
   public static void before() throws Exception {
      String serverDir = System.getProperty("server1.dist");

      JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
      jar.addClass(LocalTestServerTask.class);
      jar.addAsServiceProvider(ServerTask.class, LocalTestServerTask.class);

      jar.as(ZipExporter.class).exportTo(
              new File(serverDir, "/standalone/deployments/custom-task.jar"), true);
   }

   @Test
   @WithRunningServer({@RunningServer(name = "standalone-customtask")})
   public void shouldModifyCacheInViaTask() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);

      rcm.getCache().put(LocalTestServerTask.TASK_EXECUTED, "false");
      rcm.getCache().execute(LocalTestServerTask.NAME, Collections.emptyMap());

      assertEquals("true", rcm.getCache().get(LocalTestServerTask.TASK_EXECUTED));
   }
}
