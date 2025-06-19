package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.LookupResult.Status.INVALID_ACTION;
import static org.infinispan.rest.framework.LookupResult.Status.INVALID_METHOD;
import static org.infinispan.rest.framework.LookupResult.Status.NOT_FOUND;

import java.util.concurrent.CompletableFuture;
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
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.security.Security;
import org.infinispan.security.impl.Authorizer;

/**
 * @since 10.0
 */
public class RestDispatcherImpl implements RestDispatcher {

   private static final CompletionStage<RestResponse> NOT_FOUND_RESPONSE =
         CompletableFuture.failedFuture(new ResourceNotFoundException());

   private static final CompletionStage<RestResponse> NOT_ALLOWED =
         CompletableFuture.failedFuture(new NotAllowedException());

   private static final CompletionStage<RestResponse> MALFORMED =
         CompletableFuture.failedFuture(new MalformedRequest("Invalid 'action' parameter supplied"));

   private static final CompletionStage<RestResponse> UNAVAILABLE =
         CompletableFuture.failedFuture(new ServiceUnavailableException("Service unavailable to handle request"));

   private final ResourceManager manager;
   private final Authorizer authorizer;
   private volatile boolean started;

   public RestDispatcherImpl(ResourceManager manager, Authorizer authorizer) {
      this.manager = manager;
      this.authorizer = authorizer;
   }

   @Override
   public void initialize() {
      started = true;
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
         return CompletableFuture.failedFuture(new MalformedRequest("Invalid action"));
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
         if (invocation.permission() != null) {
               authorizer.checkPermission(restRequest.getSubject(), invocation.permission(), invocation.name(), invocation.auditContext());
            }
            if (restRequest.getSubject() != null) {
               if (invocation.requireCacheManagerStart() && !started)
                  return UNAVAILABLE;
               return Security.doAs(restRequest.getSubject(), invocation.handler(), restRequest);
         } else {
            if (invocation.requireCacheManagerStart() && !started)
               return UNAVAILABLE;
            return invocation.handler().apply(restRequest);
         }
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }
}
