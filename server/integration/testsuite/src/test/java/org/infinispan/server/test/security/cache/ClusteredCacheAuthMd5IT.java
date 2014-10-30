package org.infinispan.server.test.security.cache;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * 
 * ClusteredCacheAuthMd5IT test authentication and authorization with distributed cache and state transfer.
 * Test scenario is as follows:
 * 1. Start ISPN server
 * 2. Start second ISPN server and form cluster
 * 3. Authenticate via HR client to second server
 * 4. Shut down second server
 * 5. Do operation on remote cache via HR and verify it authorization works as expected. This remote operation
 *    happens on the first server. 
 * 
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({@RunningServer(name="hotrodAuthClustered")})
public class ClusteredCacheAuthMd5IT extends HotRodSaslAuthTestBase {
   
   private static final String ARQ_CONTAINER_ID = "hotrodAuthClustered-2";

   @ArquillianResource
   public ContainerController controller;

   @InfinispanResource("hotrodAuthClustered")
   RemoteInfinispanServer server;

   @InfinispanResource("hotrodAuthClustered-2")
   RemoteInfinispanServer server2;
   
   @Override
   public String getTestedMech() {
      return "DIGEST-MD5";
   }

   @Override
   public String getHRServerHostname() {
      return server2.getHotrodEndpoint().getInetAddress().getHostName();
   }

   @Override
   public int getHRServerPort() {
      return server2.getHotrodEndpoint().getPort();
   }
   
   @Override
   public void initAsAdmin() {
      controller.start(ARQ_CONTAINER_ID);
      initialize(ADMIN_LOGIN, ADMIN_PASSWD);
      controller.stop(ARQ_CONTAINER_ID);
   }

   @Override
   public void initAsReader() {
      controller.start(ARQ_CONTAINER_ID);
      initialize(READER_LOGIN, READER_PASSWD);
      controller.stop(ARQ_CONTAINER_ID);
   }

   @Override
   public void initAsWriter() {
      controller.start(ARQ_CONTAINER_ID);
      initialize(WRITER_LOGIN, WRITER_PASSWD);
      controller.stop(ARQ_CONTAINER_ID);
   }

   @Override
   public void initAsSupervisor() {
      controller.start(ARQ_CONTAINER_ID);
      initialize(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD);
      controller.stop(ARQ_CONTAINER_ID);
   }
}
