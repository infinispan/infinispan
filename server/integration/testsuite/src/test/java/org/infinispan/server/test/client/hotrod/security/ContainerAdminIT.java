package org.infinispan.server.test.client.hotrod.security;

import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.ADMIN_PASSWD;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.WRITER_LOGIN;
import static org.infinispan.server.test.client.hotrod.security.HotRodSaslAuthTestBase.WRITER_PASSWD;

import java.io.IOException;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.security.SecurityConfigurationHelper;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @since 9.2
 */
@RunWith(Arquillian.class)
@Category(Security.class)
@WithRunningServer({@RunningServer(name = "hotrodAuthClustered"), @RunningServer(name = "hotrodAuthClustered-2")})
public class ContainerAdminIT {

   @InfinispanResource("hotrodAuthClustered")
   RemoteInfinispanServer server1;

   @InfinispanResource("hotrodAuthClustered-2")
   RemoteInfinispanServer server2;

   private static RemoteCacheManager adminRCM = null;

   @Before
   public void prepareAdminRCM() {
      if (adminRCM == null) {
         SecurityConfigurationHelper config = new SecurityConfigurationHelper("DIGEST-MD5");
         config.forIspnServer(server1).withServerName("node0");
         config.forCredentials(ADMIN_LOGIN, ADMIN_PASSWD);
         adminRCM = new RemoteCacheManager(config.build(), true);
      }
   }

   @AfterClass
   public static void stopAdminRCM() {
      if (adminRCM != null) {
         adminRCM.stop();
         adminRCM = null;
      }
   }

   @Test
   public void testAdminOp() throws Exception {
      adminRCM.administration().getOrCreateCache("testAdminOp", "template");
   }

   @Test(expected = HotRodClientException.class)
   public void testAdminOpWithoutAdminPerm() throws IOException {
      SecurityConfigurationHelper config = new SecurityConfigurationHelper("DIGEST-MD5");
      config.forIspnServer(server1).withServerName("node0");
      config.forCredentials(WRITER_LOGIN, WRITER_PASSWD);
      RemoteCacheManager writerRCM = new RemoteCacheManager(config.build(), true);

      try {
         writerRCM.administration().getOrCreateCache("testAdminOpWithoutAdminPerm", "template");
      } finally {
         writerRCM.stop();
      }
   }
}
