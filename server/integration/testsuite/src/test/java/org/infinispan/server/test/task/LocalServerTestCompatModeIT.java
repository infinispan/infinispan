package org.infinispan.server.test.task;

import static org.infinispan.server.test.task.servertask.PriceTask.ACTION_PARAM;
import static org.infinispan.server.test.task.servertask.PriceTask.AVG_PRICE;
import static org.infinispan.server.test.task.servertask.PriceTask.TICKER_PARAM;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.Task;
import org.infinispan.server.test.task.servertask.PriceTask;
import org.infinispan.server.test.task.servertask.SpotPrice;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.tasks.ServerTask;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for server tasks with compat mode and deployed entities.
 *
 * @since 9.2
 */
@RunWith(Arquillian.class)
@Category({Task.class})
@SuppressWarnings("all")
public class LocalServerTestCompatModeIT {

   static final String DEPLOY_PATH = "/standalone/deployments/price-task.jar";

   static final String INFINISPAN_SERVER = "standalone-deployed-pojos";

   @InfinispanResource(INFINISPAN_SERVER)
   RemoteInfinispanServer server;

   @BeforeClass
   public static void before() throws Exception {
      String serverDir = System.getProperty("server1.dist");

      JavaArchive jar = ShrinkWrap.create(JavaArchive.class)
            .addClass(SpotPrice.class).addClass(PriceTask.class)
            .addAsServiceProvider(ServerTask.class, PriceTask.class);

      File f = new File(serverDir, DEPLOY_PATH);
      jar.as(ZipExporter.class).exportTo(f, true);
   }

   @AfterClass
   public static void undeploy() {
      String serverDir = System.getProperty("server1.dist");
      File jar = new File(serverDir, DEPLOY_PATH);
      if (jar.exists())
         jar.delete();

      File f = new File(serverDir, DEPLOY_PATH + ".deployed");
      if (f.exists())
         f.delete();
   }

   @Test
   @WithRunningServer({@RunningServer(name = INFINISPAN_SERVER)})
   public void shouldRunPriceTask() throws Exception {
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      RemoteCache remoteCache = rcm.getCache();
      Instant now = Instant.now();
      remoteCache.put(1, new SpotPrice("RHT", now, 500.0f));
      remoteCache.put(2, new SpotPrice("RHT", now.plus(15, ChronoUnit.MINUTES), 500.0f));
      remoteCache.put(3, new SpotPrice("RHT", now.plus(30, ChronoUnit.MINUTES), 515.0f));
      remoteCache.put(4, new SpotPrice("RHT", now.plus(45, ChronoUnit.MINUTES), 518.67f));
      remoteCache.put(5, new SpotPrice("GOOG", now.plus(48, ChronoUnit.MINUTES), 123.67f));
      remoteCache.put(6, new SpotPrice("RHT", now.plus(60, ChronoUnit.MINUTES), 523.2f));
      remoteCache.put(7, new SpotPrice("RHT", now.plus(75, ChronoUnit.MINUTES), 520.1f));
      remoteCache.put(8, new SpotPrice("RHT", now.plus(90, ChronoUnit.MINUTES), 504.0f));
      remoteCache.put(9, new SpotPrice("GOOG", now.plus(94, ChronoUnit.MINUTES), 223f));

      Map<String, String> params = new HashMap<>();
      params.put(TICKER_PARAM, "RHT");
      params.put(ACTION_PARAM, AVG_PRICE);

      Double avg = (Double) remoteCache.execute(PriceTask.NAME, params);
      assertEquals(511d, avg, 1d);
   }

}
