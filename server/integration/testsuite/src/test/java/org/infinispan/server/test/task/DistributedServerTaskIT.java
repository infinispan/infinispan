package org.infinispan.server.test.task;

import static java.util.Arrays.asList;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Task;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(Arquillian.class)
@Category({Task.class})
@WithRunningServer({@RunningServer(name="clusteredcache-1"), @RunningServer(name = "clusteredcache-2")})
public class DistributedServerTaskIT extends AbstractDistributedServerTaskIT {

   @InfinispanResource("clusteredcache-1")
   RemoteInfinispanServer server1;

   @InfinispanResource("clusteredcache-2")
   RemoteInfinispanServer server2;


   @Override
   protected List<RemoteInfinispanServer> getServers() {
      List<RemoteInfinispanServer> servers = new ArrayList<>();
      servers.add(server1);
      servers.add(server2);

      return Collections.unmodifiableList(servers);
   }

   @BeforeClass
   public static void before() throws Exception {
      String[] serverDirs = new String[]{System.getProperty("server1.dist"), System.getProperty("server2.dist")};

      JavaArchive jar = createJavaArchive();
      jar.addAsResource(new File("/stream_serverTask.js"));
      jar.addAsManifestResource("MANIFEST.MF");

      for (String serverDir : serverDirs) {
         File f = new File(serverDir, "/standalone/deployments/custom-distributed-task.jar");
         jar.as(ZipExporter.class).exportTo(f, true);
      }

      expectedServerList = asList("node0", "node1");
   }

   @AfterClass
   public static void undeploy() {
      String[] serverDirs = new String[] {System.getProperty("server1.dist"), System.getProperty("server2.dist")};
      for(String serverDir : serverDirs) {
         File jar = new File(serverDir, "/standalone/deployments/custom-distributed-task.jar");
         if (jar.exists())
            jar.delete();
         File f = new File(serverDir, "/standalone/deployments/custom-distributed-task.jar.deployed");
         if (f.exists())
            f.delete();
      }
   }
}
