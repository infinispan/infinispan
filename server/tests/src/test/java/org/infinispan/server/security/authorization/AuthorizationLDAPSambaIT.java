package org.infinispan.server.security.authorization;

import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.artifacts.Artifacts;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.jupiter.InfinispanSuite;
import org.infinispan.testing.jupiter.tags.Security;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({AuthorizationLDAPSambaIT.HotRod.class, AuthorizationLDAPSambaIT.Resp.class, AuthorizationLDAPSambaIT.Rest.class})
@Security
public class AuthorizationLDAPSambaIT extends InfinispanSuite {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationLDAPSambaTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(Artifacts.artifacts())
               .property(TestSystemPropertyNames.LDAP_SERVER, "samba")
               .property(TestSystemPropertyNames.HOTROD_CLIENT_SASL_MECHANISM, "PLAIN")
               .addListener(new LdapServerListener())
               .build();

   static class HotRod extends HotRodAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPSambaIT.SERVERS;

      public HotRod() {
         super(SERVERS);
      }
   }

   static class Rest extends RESTAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPSambaIT.SERVERS;

      public Rest() {
         super(SERVERS, TestUser::getUser, user -> {
            RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
            if (user.getUser() != null) {
               restBuilder.security().authentication()
                     .mechanism("BASIC")
                     .username(user.getUser())
                     .password(user.getPassword());
            }
            return restBuilder;
         });
      }
   }

   static class Resp extends RESPAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPSambaIT.SERVERS;
      public Resp() {
         super(SERVERS);
      }
   }
}
