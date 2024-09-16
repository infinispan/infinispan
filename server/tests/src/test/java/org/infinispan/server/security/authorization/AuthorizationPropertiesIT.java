package org.infinispan.server.security.authorization;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({AuthorizationPropertiesIT.HotRod.class, AuthorizationPropertiesIT.Resp.class, AuthorizationPropertiesIT.Rest.class})
public class AuthorizationPropertiesIT extends InfinispanSuite {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthorizationPropertiesTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(ClusteredIT.artifacts())
               .numServers(2)
               .build();

   static class HotRod extends HotRodAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationPropertiesIT.SERVERS;

      public HotRod() {
         super(SERVERS);
      }
   }

   static class Resp extends RESPAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationPropertiesIT.SERVERS;
      public Resp() {
         super(SERVERS);
      }
   }

   static class Rest extends RESTAuthorizationTest {
      @RegisterExtension
      static InfinispanServerExtension SERVERS = AuthorizationPropertiesIT.SERVERS;

      public Rest() {
         super(SERVERS);
      }

      @Test
      public void testListUsers() {
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
         Json realmPrincipals = Json.read(assertStatus(OK, client.raw().get("/rest/v2/security/users")));
         List<Json> users = realmPrincipals.asJsonMap().get("default:properties").asJsonList();
         assertEquals(users.size(), 21);
      }
   }
}
