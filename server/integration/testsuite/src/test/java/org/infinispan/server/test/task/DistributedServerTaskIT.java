package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.server.test.category.Task;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collections;
import java.util.List;

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

   @BeforeClass
   public static void before() throws Exception {
      String[] serverDirs = new String[]{System.getProperty("server1.dist"), System.getProperty("server2.dist")};

      JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
      jar.addClass(DistributedTestServerTask.class);
      jar.addAsServiceProvider(ServerTask.class, DistributedTestServerTask.class);

      for (String serverDir : serverDirs) {
         jar.as(ZipExporter.class).exportTo(
                 new File(serverDir, "/standalone/deployments/custom-distributed-task.jar"), true);
      }
   }

   @Before
   public void setUp() {
      if (rcm1 == null) {
         Configuration conf = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
                 .port(server1.getHotrodEndpoint().getPort()).build();
         rcm1 = new RemoteCacheManager(conf);
      }
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
}
