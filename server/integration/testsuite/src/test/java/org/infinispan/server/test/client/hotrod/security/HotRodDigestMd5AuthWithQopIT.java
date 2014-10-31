package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.security.SaslConfigurationBuilder;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 *
 * HotRodDigestMd5AuthWithQopIT tests DIGEST-MD5 SASL authentication of HotRod client with an encrypted connection
 * as specified by the SASL mechanism.
 *
 * @author vjuranek
 * @author Tristan Tarrant
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
@WithRunningServer({@RunningServer(name="hotrodAuthQop")})
public class HotRodDigestMd5AuthWithQopIT extends HotRodSaslAuthTestBase {
   
   @InfinispanResource("hotrodAuthQop")
   RemoteInfinispanServer server;

   @Override
   protected SaslConfigurationBuilder getDefaultSaslConfigBuilder() {
      SaslConfigurationBuilder builder = super.getDefaultSaslConfigBuilder();
      builder.withDefaultQop();
      return builder;
   }

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
