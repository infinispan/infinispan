package org.infinispan.rest.resources;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;

/**
 * @since 15.0
 */
@Test(groups = "functional", testName = "rest.AccessManagementResourceTest")
public class AccessManagementResourceTest extends AbstractRestResourceTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new AccessManagementResourceTest().withSecurity(true).protocol(HTTP_11).ssl(false).browser(false),
            new AccessManagementResourceTest().withSecurity(true).protocol(HTTP_20).ssl(false).browser(false),
      };
   }

   @Test
   public void testRolesList() {
      CompletionStage<RestResponse> response = adminClient.security().listRoles();
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.isArray()).isTrue();
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("ADMIN", "USER");

      response = adminClient.security().listRoles(false);
      jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.isArray()).isTrue();
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("ADMIN", "USER");
   }

   @Test
   public void testDescribeRole() {
      CompletionStage<RestResponse> response = adminClient.security().describeRole("ADMIN");
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.at("name").asString()).isEqualTo("ADMIN");
      assertThat(jsonNode.at("permissions").asList()).containsExactly("ALL");
      assertThat(jsonNode.at("description").asString()).contains("admin role");
      assertThat(jsonNode.at("implicit").asBoolean()).isFalse();
   }

   @Test
   public void testDetailedRolesList() {
      CompletionStage<RestResponse> response = adminClient.security().listRoles(true);
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      Json jsonNode = Json.read(join(response).getBody());
      assertThat(jsonNode.asJsonMap()).hasSize(2);
      assertThat(jsonNode.at("ADMIN").at("permissions").asList()).containsExactly("ALL");
      assertThat(jsonNode.at("ADMIN").at("inheritable").asBoolean()).isTrue();
      assertThat(jsonNode.at("ADMIN").at("implicit").asBoolean()).isFalse();
      assertThat(jsonNode.at("ADMIN").at("description").asString()).contains("admin role");
      assertThat(jsonNode.at("USER").at("permissions").asList())
            .containsExactlyInAnyOrder("READ", "WRITE", "BULK_READ", "EXEC");
      assertThat(jsonNode.at("USER").at("inheritable").asBoolean()).isTrue();
      assertThat(jsonNode.at("USER").at("implicit").asBoolean()).isFalse();
      assertThat(jsonNode.at("USER").at("description").asString()).contains("user role");
   }
}
