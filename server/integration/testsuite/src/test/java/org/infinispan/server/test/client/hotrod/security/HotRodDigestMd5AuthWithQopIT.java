package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SaslQop;
import org.infinispan.client.hotrod.configuration.SaslStrength;
import org.infinispan.server.test.category.Security;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.After;
import org.junit.Before;
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
public class HotRodDigestMd5AuthWithQopIT extends HotRodSaslAuthTestBase {

   private static final String ARQ_CONTAINER_ID = "hotrodAuthQop";

   @ArquillianResource
   public ContainerController controller;

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

   @Before
   public void startIspnServer() {
      controller.start(ARQ_CONTAINER_ID);
   }

   @After
   public void stopIspnServer() {
      controller.stop(ARQ_CONTAINER_ID);
   }

   @Override
   public String getHRServerHostname() {
      return "localhost";
   }

   @Override
   public int getHRServerPort() {
      return 11222;
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
