package org.infinispan.server.security.authorization;

import java.net.InetSocketAddress;

import javax.security.auth.Subject;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.security.VoidCallbackHandler;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
@Suite(failIfNoTests = false)
@SelectClasses({AuthorizationKerberosIT.HotRod.class, AuthorizationKerberosIT.Rest.class})
@Security
public class AuthorizationKerberosIT extends InfinispanSuite {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationKerberosTest.xml")
               .numServers(1)
               .property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf")
               .addListener(new LdapServerListener(true))
               .runMode(ServerRunMode.EMBEDDED)
               .build();

   static class HotRod extends HotRodAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationKerberosIT.SERVERS;

      public HotRod() {
         super(SERVERS, AuthorizationKerberosIT::expectedServerPrincipalName, user -> {
            ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
            if (user != TestUser.ANONYMOUS) {
               Subject subject = Common.createSubject(user.getUser(), "INFINISPAN.ORG", user.getPassword().toCharArray());
               hotRodBuilder.security().authentication()
                     .saslMechanism("GSSAPI")
                     .serverName("datagrid")
                     .realm("default")
                     .callbackHandler(new VoidCallbackHandler())
                     .clientSubject(subject);
            }
            return hotRodBuilder;
         });
      }
   }

   static class Rest extends RESTAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationKerberosIT.SERVERS;

      public Rest() {
         super(SERVERS, AuthorizationKerberosIT::expectedServerPrincipalName, user -> {
            RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
            if (user != TestUser.ANONYMOUS) {
               Subject subject = Common.createSubject(user.getUser(), "INFINISPAN.ORG", user.getPassword().toCharArray());
               restBuilder.security().authentication()
                     .mechanism("SPNEGO")
                     .clientSubject(subject);
               // Kerberos is strict about the hostname, so we do this by hand
               InetSocketAddress serverAddress = SERVERS.getServerDriver().getServerSocket(0, 11222);
               restBuilder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
            }
            return restBuilder;
         });
      }
   }

   private static String expectedServerPrincipalName(TestUser user) {
      return String.format("%s@INFINISPAN.ORG", user.getUser());
   }
}
