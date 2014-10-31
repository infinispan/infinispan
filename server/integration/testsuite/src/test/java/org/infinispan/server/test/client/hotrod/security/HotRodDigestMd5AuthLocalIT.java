package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * 
 * HotRodDigestMd5AuthLocalIT tests DIGEST-MD5 SASL authentication of HotRod client against local cache.
 * 
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
public class HotRodDigestMd5AuthLocalIT extends HotRodSaslAuthTestBase {

   @InfinispanResource("hotrodAuth")
   RemoteInfinispanServer server;

   @Override
   public String getTestedMech() {
      return "DIGEST-MD5";
   }

   @Override
   public RemoteInfinispanServer getRemoteServer() {
      return server;
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
