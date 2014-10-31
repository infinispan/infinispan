package org.infinispan.server.test.util.security;

import java.io.File;

import javax.security.auth.Subject;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SaslQop;
import org.infinispan.client.hotrod.configuration.SaslStrength;
import org.infinispan.server.test.util.ITestUtils;

/**
 * 
 * SaslConfigurationBuilder is a convenient class for various security tests which provides remote
 * configuration builders for various means of authentication.
 * 
 * @author vjuranek
 * @since 7.0
 */
public class SaslConfigurationBuilder extends ConfigurationBuilder {

   protected static final String DEFAULT_TEST_REALM = "ApplicationRealm";

   protected static final String DEFAULT_KEYSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
         + "keystore_client.jks";
   protected static final String DEFAULT_KEYSTORE_PASSWORD = "secret";
   protected static final String DEFAULT_TRUSTSTORE_PATH = ITestUtils.SERVER_CONFIG_DIR + File.separator
         + "truststore_client.jks";
   protected static final String DEFAULT_TRUSTSTORE_PASSWORD = "secret";

   private final String saslMech;

   public SaslConfigurationBuilder(String saslMech) {
      this.saslMech = saslMech;
   }

   public SaslConfigurationBuilder forCredentials(String login, String password) {
      this.security().authentication().callbackHandler(new SimpleLoginHandler(login, password, DEFAULT_TEST_REALM));
      return this;
   }

   public SaslConfigurationBuilder forCredentials(String login, String password, String realm) {
      this.security().authentication().callbackHandler(new SimpleLoginHandler(login, password, realm));
      return this;
   }

   public SaslConfigurationBuilder forSubject(Subject subj) {
      this.security().authentication().clientSubject(subj).callbackHandler(new SimpleLoginHandler("", "")); //callback handle is required by ISPN config validation
      return this;
   }

   public SaslConfigurationBuilder withDefaultSsl() {
      this.security().ssl().enable()
            .keyStoreFileName(DEFAULT_KEYSTORE_PATH)
            .keyStorePassword(DEFAULT_KEYSTORE_PASSWORD.toCharArray())
            .trustStoreFileName(DEFAULT_TRUSTSTORE_PATH)
            .trustStorePassword(DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
      return this;
   }

   public SaslConfigurationBuilder withDefaultQop() {
      this.security().authentication().saslQop(SaslQop.AUTH_CONF)
            .saslStrength(SaslStrength.HIGH, SaslStrength.MEDIUM, SaslStrength.LOW);
      return this;
   }

   public SaslConfigurationBuilder forIspnServer(RemoteInfinispanServer ispnServer) {
      String hostname = ispnServer.getHotrodEndpoint().getInetAddress().getHostName();
      this.addServer().host(hostname).port(ispnServer.getHotrodEndpoint().getPort());
      this.security().authentication().saslMechanism(saslMech).enable();
      return this;
   }

   public SaslConfigurationBuilder forIspnServer(String hostname, int port) {
      this.addServer().host(hostname).port(port);
      this.security().authentication().saslMechanism(saslMech).enable();
      return this;
   }

   public SaslConfigurationBuilder forIspnServer(String hostname, int port, String serverName) {
      this.addServer().host(hostname).port(port);
      this.security().authentication().serverName(serverName).saslMechanism(saslMech).enable();
      return this;
   }

   public SaslConfigurationBuilder withServerName(String serverName) {
      this.security().authentication().serverName(serverName);
      return this;
   }

}
