package org.infinispan.client.rest;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface RestSecurityClient {
   CompletionStage<RestResponse> listUsers();

   CompletionStage<RestResponse> listRoles();

   CompletionStage<RestResponse> listRoles(boolean detailed);

   CompletionStage<RestResponse> listRoles(String principal);

   CompletionStage<RestResponse> grant(String principal, List<String> roles);

   CompletionStage<RestResponse> deny(String principal, List<String> roles);

   CompletionStage<RestResponse> flushCache();

   CompletionStage<RestResponse> createRole(String name, String description, List<String> permissions);

   CompletionStage<RestResponse> removeRole(String name);

   CompletionStage<RestResponse> describeRole(String name);

   CompletionStage<RestResponse> listPrincipals();

   CompletionStage<RestResponse> listPrincipals(boolean detailed);
}
