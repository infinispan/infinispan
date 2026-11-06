package org.infinispan.server.security.authorization;

import org.infinispan.server.functional.ClusteredIT;
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
@SelectClasses({AuthorizationLDAPDs389IT.HotRod.class, AuthorizationLDAPDs389IT.Resp.class, AuthorizationLDAPDs389IT.Rest.class})
@Security
public class AuthorizationLDAPDs389IT extends InfinispanSuite {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationLDAPDs389Test.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(Artifacts.artifacts())
               .property(TestSystemPropertyNames.LDAP_SERVER, "ds389")
               .property(TestSystemPropertyNames.HOTROD_CLIENT_SASL_MECHANISM, "SCRAM-SHA-512")
               .addListener(new LdapServerListener())
               .build();

   static class HotRod extends HotRodAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPDs389IT.SERVERS;

      public HotRod() {
         super(SERVERS);
      }
   }

   static class Rest extends RESTAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPDs389IT.SERVERS;

      public Rest() {
         super(SERVERS);
      }
   }

   static class Resp extends RESPAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPDs389IT.SERVERS;
      public Resp() {
         super(SERVERS);
      }
   }
}
