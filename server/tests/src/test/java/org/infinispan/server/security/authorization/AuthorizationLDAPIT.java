package org.infinispan.server.security.authorization;

import org.infinispan.server.functional.ClusteredIT;
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
 * @since 10.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({AuthorizationLDAPIT.HotRod.class, AuthorizationLDAPIT.Resp.class, AuthorizationLDAPIT.Rest.class})
@Security
public class AuthorizationLDAPIT extends InfinispanSuite {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationLDAPTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(ClusteredIT.artifacts())
               .addListener(new LdapServerListener())
               .build();

   static class HotRod extends HotRodAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPIT.SERVERS;

      public HotRod() {
         super(SERVERS);
      }
   }

   static class Rest extends RESTAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPIT.SERVERS;

      public Rest() {
         super(SERVERS);
      }
   }

   static class Resp extends RESPAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationLDAPIT.SERVERS;
      public Resp() {
         super(SERVERS);
      }
   }
}
