package org.infinispan.server.security.authorization;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

@RunWith(AuthorizationSuiteRunner.class)
@Suite.SuiteClasses({HotRodAuthorizationTest.class, RESPAuthorizationTest.class, RESTAuthorizationTest.class})
@Category(Security.class)
public class AuthorizationPropertiesIT extends AbstractAuthorization {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthorizationPropertiesTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts(ClusteredIT.mavenArtifacts())
               .artifacts(ClusteredIT.artifacts())
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Override
   protected InfinispanServerRule getServers() {
      return SERVERS;
   }

   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }

   @Test
   public void testListPrincipals() {
      RestClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      Json realmPrincipals = Json.read(assertStatus(OK, client.raw().get("/rest/v2/security/principals")));
      List<Json> principals = realmPrincipals.asJsonMap().get("default:properties").asJsonList();
      assertEquals(21, principals.size());
   }
}
