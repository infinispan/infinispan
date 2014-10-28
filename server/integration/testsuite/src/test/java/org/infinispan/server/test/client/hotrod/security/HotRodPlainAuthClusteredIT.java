package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 *
 * HotRodPlainAuthIT tests PLAIN SASL authentication of HotRod client.
 *
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
public class HotRodPlainAuthClusteredIT extends HotRodSaslAuthTestBase {

   @InfinispanResource("hotrodAuthClustered")
   RemoteInfinispanServer server;

   @Override
   public String getTestedMech() {
      return "PLAIN";
   }

   @Override
   public String getHRServerHostname() {
      return server.getHotrodEndpoint().getInetAddress().getHostName();
   }

   @Override
   public int getHRServerPort() {
      return server.getHotrodEndpoint().getPort();
   }

   @Override
   public void initAsAdmin() {
      initialize(ADMIN_LOGIN, ADMIN_PASSWD);
   }

   @Override
   public void initAsReader() {
      initialize(READER_LOGIN, READER_PASSWD);
   }

   @Override
   public void initAsWriter() {
      initialize(WRITER_LOGIN, WRITER_PASSWD);
   }

   @Override
   public void initAsSupervisor() {
      initialize(SUPERVISOR_LOGIN, SUPERVISOR_PASSWD);
   }
}
