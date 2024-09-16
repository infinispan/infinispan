package org.infinispan.server.security;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Security
public class AuthenticationAggregateRealmIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationAggregateRealm.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testAggregate() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      SERVERS.getServerDriver().applyKeyStore(builder, "admin.pfx");
      builder
            .security()
            .ssl()
            .sniHostName("infinispan")
            .hostnameVerifier((hostname, session) -> true).connectionTimeout(50_000).socketTimeout(50_000);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).get();
      Json acl = Json.read(assertStatus(OK, client.raw().get("/rest/v2/security/user/acl")));
      Json subject = acl.asJsonMap().get("subject");

      List<Object> names = subject.asJsonList().stream().map(j -> j.asMap().get("name")).collect(Collectors.toList());
      assertEquals(4, names.size());
      assertTrue(names.contains("CN=admin,OU=Infinispan,O=JBoss,L=Red Hat"));
      assertTrue(names.contains("___script_manager"));
      assertTrue(names.contains("admin"));
      assertTrue(names.contains("___schema_manager"));
   }
}
