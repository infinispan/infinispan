package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSecurityClient;

import okhttp3.Request;
import okhttp3.internal.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class RestSecurityClientOkHttp implements RestSecurityClient {

   private final RestClientOkHttp client;
   private final String baseSecurityURL;

   public RestSecurityClientOkHttp(RestClientOkHttp restClient) {
      this.client = restClient;
      this.baseSecurityURL = String.format("%s%s/v2/security", restClient.getBaseURL(), restClient.getConfiguration().contextPath());
   }

   @Override
   public CompletionStage<RestResponse> listPrincipals() {
      return client.execute(baseSecurityURL, "principals");
   }

   @Override
   public CompletionStage<RestResponse> listRoles(String principal) {
      return client.execute(baseSecurityURL, "roles", principal);
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
      Request.Builder builder = new Request.Builder();
      StringBuilder sb = new StringBuilder(baseSecurityURL);
      sb.append("/roles/").append(principal).append("?action=").append(action);
      for (String role : roles) {
         sb.append("&role=").append(role);
      }
      builder.url(sb.toString()).put(Util.EMPTY_REQUEST);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> flushCache() {
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(baseSecurityURL + "/cache?action=flush");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> createRole(String name, List<String> permissions) {
      Request.Builder builder = new Request.Builder();
      StringBuilder sb = new StringBuilder(baseSecurityURL);
      sb.append("/permissions/").append(name).append('?');
      for (int i = 0; i < permissions.size(); i++) {
         if (i > 0) {
            sb.append('&');
         }
         sb.append("permission=").append(permissions.get(i));
      }
      builder.url(sb.toString()).put(Util.EMPTY_REQUEST);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> removeRole(String name) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseSecurityURL + "/permissions/" + name);
      return client.execute(builder.delete());
   }

   @Override
   public CompletionStage<RestResponse> describeRole(String name) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseSecurityURL + "/permissions/" + name);
      return client.execute(builder.get());
   }
}
