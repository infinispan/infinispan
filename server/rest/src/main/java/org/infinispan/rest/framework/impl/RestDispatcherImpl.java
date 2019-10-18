package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.LookupResult.Status.INVALID_ACTION;
import static org.infinispan.rest.framework.LookupResult.Status.INVALID_METHOD;
import static org.infinispan.rest.framework.LookupResult.Status.NOT_FOUND;

import java.util.concurrent.CompletionStage;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.operations.exceptions.MalformedRequest;
import org.infinispan.rest.operations.exceptions.NotAllowedException;
import org.infinispan.rest.operations.exceptions.ResourceNotFoundException;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * @since 10.0
 */
public class RestDispatcherImpl implements RestDispatcher {

   private static final CompletionStage<RestResponse> NOT_FOUND_RESPONSE =
         CompletableFutures.completedExceptionFuture(new ResourceNotFoundException());

   private static final CompletionStage<RestResponse> NOT_ALLOWED =
         CompletableFutures.completedExceptionFuture(new NotAllowedException());

   private static final CompletionStage<RestResponse> MALFORMED =
         CompletableFutures.completedExceptionFuture(new MalformedRequest("Invalid 'action' parameter supplied"));

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

      LookupResult.Status status = lookupResult.getStatus();

      if (status.equals(NOT_FOUND)) {
         return NOT_FOUND_RESPONSE;
      }

      if (status.equals(INVALID_METHOD)) {
         return NOT_ALLOWED;
      }

      if (status.equals(INVALID_ACTION)) {
         return MALFORMED;
      }

      restRequest.setVariables(lookupResult.getVariables());
      Invocation invocation = lookupResult.getInvocation();

      try {
         return invocation.handler().apply(restRequest);
      } catch (Throwable t) {
         return CompletableFutures.completedExceptionFuture(t);
      }
   }
}
