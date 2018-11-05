package org.infinispan.rest.framework.impl;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.operations.exceptions.MalformedRequest;
import org.infinispan.rest.operations.exceptions.ResourceNotFoundException;

/**
 * @since 10.0
 */
public class RestDispatcherImpl implements RestDispatcher {

   private final ResourceManager manager;

   public RestDispatcherImpl(ResourceManager manager) {
      this.manager = manager;
   }

   @Override
   public RestResponse dispatch(RestRequest restRequest) {
      String action = restRequest.getAction();
      if (action != null && action.isEmpty()) throw new MalformedRequest("Invalid action");

      LookupResult lookupResult = manager.lookupResource(restRequest.method(), restRequest.path(), restRequest.getAction());

      if (lookupResult == null) {
         throw new ResourceNotFoundException();
      }

      restRequest.setVariables(lookupResult.getVariables());
      Invocation invocation = lookupResult.getInvocation();
      if (invocation == null) {
         throw new ResourceNotFoundException();
      }

      return invocation.handler().apply(restRequest);
   }

}
