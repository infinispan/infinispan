package org.infinispan.rest.resources;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
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
      Json jsonNode = Json.read(join(response).body());
      assertThat(jsonNode.isArray()).isTrue();
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("ADMIN", "USER");

      response = adminClient.security().listRoles(false);
      jsonNode = Json.read(join(response).body());
      assertThat(jsonNode.isArray()).isTrue();
      assertThat(jsonNode.asList()).containsExactlyInAnyOrder("ADMIN", "USER");
   }

   @Test
   public void testDescribeRole() {
      CompletionStage<RestResponse> response = adminClient.security().describeRole("ADMIN");
      ResponseAssertion.assertThat(response).isOk();
      Json jsonNode = Json.read(join(response).body());
      assertThat(jsonNode.at("name").asString()).isEqualTo("ADMIN");
      assertThat(jsonNode.at("permissions").asList()).containsExactly("ALL");
      assertThat(jsonNode.at("description").asString()).contains("admin role");
      assertThat(jsonNode.at("implicit").asBoolean()).isFalse();
   }

   @Test
   public void testCRUDRole() {
      CompletionStage<RestResponse> createRoleResponse = adminClient.security().createRole("NEW_ROLE", "something", List.of("ALL"));
      ResponseAssertion.assertThat(createRoleResponse).isOk();
      // second time fails!
      createRoleResponse = adminClient.security().createRole("NEW_ROLE", "something", List.of("ALL"));
      ResponseAssertion.assertThat(createRoleResponse).isConflicted();

      CompletionStage<RestResponse> readNewRole = adminClient.security().describeRole("NEW_ROLE");
      ResponseAssertion.assertThat(readNewRole).isOk();
      ResponseAssertion.assertThat(readNewRole).hasContentType("application/json");
      Json jsonRole = Json.read(join(readNewRole).body());
      assertRoleJson(jsonRole, "ALL", "something");

      CompletionStage<RestResponse> updateResponse = adminClient.security().updateRole("NEW_ROLE", "desUpdate", Collections.emptyList());
      ResponseAssertion.assertThat(updateResponse).isOk();

      CompletionStage<RestResponse> updatedRole = adminClient.security().describeRole("NEW_ROLE");
      Json jsonUpdatedRole = Json.read(join(updatedRole).body());
      assertRoleJson(jsonUpdatedRole, "ALL", "desUpdate");

      updateResponse = adminClient.security().updateRole("NEW_ROLE", "", Collections.singletonList("READ"));
      ResponseAssertion.assertThat(updateResponse).isOk();

      updatedRole = adminClient.security().describeRole("NEW_ROLE");
      jsonUpdatedRole = Json.read(join(updatedRole).body());
      assertRoleJson(jsonUpdatedRole, "READ", "desUpdate");

      CompletionStage<RestResponse> removeRole = adminClient.security().removeRole("NEW_ROLE");
      ResponseAssertion.assertThat(removeRole).isOk();
      ResponseAssertion.assertThat(adminClient.security().describeRole("NEW_ROLE")).isNotFound();
   }

   private static void assertRoleJson(Json jsonRole, String permission, String something) {
      assertThat(jsonRole.at("name").asString()).isEqualTo("NEW_ROLE");
      assertThat(jsonRole.at("permissions").asList()).containsExactly(permission);
      assertThat(jsonRole.at("description").asString()).isEqualTo(something);
      assertThat(jsonRole.at("implicit").asBoolean()).isFalse();
   }

   @Test
   public void testDetailedRolesList() {
      CompletionStage<RestResponse> response = adminClient.security().listRoles(true);
      ResponseAssertion.assertThat(response).isOk();
      ResponseAssertion.assertThat(response).hasContentType("application/json");
      Json jsonNode = Json.read(join(response).body());
      assertThat(jsonNode.asJsonMap()).hasSize(2);
      assertThat(jsonNode.at("ADMIN").at("permissions").asList()).containsExactly("ALL");
      assertThat(jsonNode.at("ADMIN").at("inheritable").asBoolean()).isTrue();
      assertThat(jsonNode.at("ADMIN").at("implicit").asBoolean()).isFalse();
      assertThat(jsonNode.at("ADMIN").at("description").asString()).contains("admin role");
      assertThat(jsonNode.at("USER").at("permissions").asList())
            .containsExactlyInAnyOrder("READ", "WRITE", "BULK_READ", "EXEC", "CREATE");
      assertThat(jsonNode.at("USER").at("inheritable").asBoolean()).isTrue();
      assertThat(jsonNode.at("USER").at("implicit").asBoolean()).isFalse();
      assertThat(jsonNode.at("USER").at("description").asString()).contains("user role");
   }
}
