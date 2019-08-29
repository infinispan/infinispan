package org.infinispan.rest.framework.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.operations.exceptions.MalformedRequest;
import org.infinispan.rest.operations.exceptions.ResourceNotFoundException;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @since 10.0
 */
public class RestDispatcherImpl implements RestDispatcher {

   private static final CompletionStage<RestResponse> NOT_FOUND_RESPONSE =
         CompletableFutures.completedExceptionFuture(new ResourceNotFoundException());

   private final ResourceManager manager;

   public RestDispatcherImpl(ResourceManager manager) {
      this.manager = manager;
   }

   @Override
   public LookupResult lookupInvocation(RestRequest restRequest) {
      return manager.lookupResource(restRequest.method(), restRequest.path(), restRequest.getAction());
   }

   @Override
   public CompletionStage<RestResponse> dispatch(RestRequest restRequest) {
      return dispatch(restRequest, lookupInvocation(restRequest));
   }

   @Override
   public CompletionStage<RestResponse> dispatch(RestRequest restRequest, LookupResult lookupResult) {
      String action = restRequest.getAction();
      if (action != null && action.isEmpty()) {
         return CompletableFutures.completedExceptionFuture(new MalformedRequest("Invalid action"));
      }

      if (lookupResult == null) {
         return NOT_FOUND_RESPONSE;
      }

      restRequest.setVariables(lookupResult.getVariables());
      Invocation invocation = lookupResult.getInvocation();
      if (invocation == null) {
         return NOT_FOUND_RESPONSE;
      }

      try {
         return invocation.handler().apply(restRequest);
      } catch (Throwable t) {
         return CompletableFutures.completedExceptionFuture(t);
      }
   }
}
