package org.infinispan.client.rest.impl.jdk;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSecurityClient;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 14.0
 **/
public class RestSecurityClientJDK implements RestSecurityClient {
   private final RestRawClientJDK client;
   private final String path;

   public RestSecurityClientJDK(RestRawClientJDK restClient) {
      this.client = restClient;
      this.path = restClient.getConfiguration().contextPath() + "/v2/security";
   }

   @Override
   public CompletionStage<RestResponse> listPrincipals() {
      return client.get(path + "/principals");
   }

   @Override
   public CompletionStage<RestResponse> listPrincipals(boolean detailed) {
      return client.get(path + "/principals" + (detailed ? "?action=detailed" : ""));
   }

   @Override
   public CompletionStage<RestResponse> listUsers() {
      return client.get(path + "/users");
   }

   @Override
   public CompletionStage<RestResponse> listRoles() {
      return client.get(path + "/roles");
   }

   @Override
   public CompletionStage<RestResponse> listRoles(boolean detailed) {
      if (detailed) {
         return client.get(path + "/roles?action=detailed");
      }
      return listRoles();
   }

   @Override
   public CompletionStage<RestResponse> listRoles(String principal) {
      Objects.requireNonNull(principal, "principal");
      return client.get(path + "/roles/" + principal);
   }

   @Override
   public CompletionStage<RestResponse> grant(String principal, List<String> roles) {
      return modifyAcl(principal, roles, "grant");
   }

   @Override
   public CompletionStage<RestResponse> deny(String principal, List<String> roles) {
      return modifyAcl(principal, roles, "deny");
   }

   private CompletionStage<RestResponse> modifyAcl(String principal, List<String> roles, String action) {
      StringBuilder sb = new StringBuilder(path);
      sb.append("/roles/").append(principal).append("?action=").append(action);
      for (String role : roles) {
         sb.append("&role=").append(role);
      }
      return client.put(sb.toString());
   }

   @Override
   public CompletionStage<RestResponse> flushCache() {
      return client.post(path + "/cache?action=flush");
   }

   @Override
   public CompletionStage<RestResponse> createRole(String name, String description, List<String> permissions) {
      return createOrUpdateRole(name, description, permissions, true);
   }

   @Override
   public CompletionStage<RestResponse> updateRole(String name, String description, List<String> permissions) {
      return createOrUpdateRole(name, description, permissions, false);
   }

   private CompletionStage<RestResponse> createOrUpdateRole(String name, String description, List<String> permissions, boolean create) {
      StringBuilder sb = new StringBuilder(path);
      sb.append("/permissions/").append(name).append('?');
      for (int i = 0; i < permissions.size(); i++) {
         if (i > 0) {
            sb.append('&');
         }
         sb.append("permission=").append(permissions.get(i));
      }
      RestEntity entity;
      if (description != null) {
         entity= RestEntity.create(MediaType.TEXT_PLAIN, description);
      } else {
         entity = RestEntity.EMPTY;
      }
      return create ? client.post(sb.toString(), entity) : client.put(sb.toString(), entity);
   }

   @Override
   public CompletionStage<RestResponse> removeRole(String name) {
      return client.delete(path + "/permissions/" + name);
   }

   @Override
   public CompletionStage<RestResponse> describeRole(String name) {
      return client.get(path + "/permissions/" + name);
   }
}
