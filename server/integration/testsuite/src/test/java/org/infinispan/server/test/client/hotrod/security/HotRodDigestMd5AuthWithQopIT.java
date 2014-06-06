package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SaslQop;
import org.infinispan.client.hotrod.configuration.SaslStrength;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 *
 * HotRodDigestMd5AuthWithQopIT tests DIGEST-MD5 SASL authentication of HotRod client with an encrypted connection
 * as specified by the SASL mechanism.
 *
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ Security.class })
public class HotRodDigestMd5AuthWithQopIT extends HotRodSaslAuthTestBase {

   @InfinispanResource("hotrodAuthQop")
   RemoteInfinispanServer server;

   @Override
   protected ConfigurationBuilder getDefaultConfigBuilder() {
      ConfigurationBuilder builder = super.getDefaultConfigBuilder();
      builder.security().authentication().saslQop(SaslQop.AUTH_CONF).saslStrength(SaslStrength.HIGH, SaslStrength.MEDIUM, SaslStrength.LOW);
      return builder;
   }

   @Override
   public String getTestedMech() {
      return "DIGEST-MD5";
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
