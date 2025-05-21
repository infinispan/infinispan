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
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/

@Category(Security.class)
@Tag("embedded")
public class AuthorizationKerberosIT extends AbstractAuthorization {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationKerberosTest.xml")
               .numServers(1)
               .property("java.security.krb5.conf", "${infinispan.server.config.path}/krb5.conf")
               .addListener(new LdapServerListener(true))
               .runMode(ServerRunMode.EMBEDDED)
               .build();

   public AuthorizationKerberosIT() {
      super(SERVERS);
   }

   @Override
   protected InfinispanServerExtension getServers() {
      return SERVERS;
   }

   @Override
   protected void addClientBuilders(TestUser user) {
      ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      if (user != TestUser.ANONYMOUS) {
         Subject subject = Common.createSubject(user.getUser(), "INFINISPAN.ORG", user.getPassword().toCharArray());
         hotRodBuilder.security().authentication()
               .saslMechanism("GSSAPI")
               .serverName("datagrid")
               .realm("default")
               .callbackHandler(new VoidCallbackHandler())
               .clientSubject(subject);
         restBuilder.security().authentication()
               .mechanism("SPNEGO")
               .clientSubject(subject);
         // Kerberos is strict about the hostname, so we do this by hand
         InetSocketAddress serverAddress = SERVERS.getServerDriver().getServerSocket(0, 11222);
         restBuilder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      hotRodBuilders.put(user, hotRodBuilder);
      restBuilders.put(user, restBuilder);
   }

   protected String expectedServerPrincipalName(TestUser user) {
      return String.format("%s@INFINISPAN.ORG", user.getUser());
   }
}
