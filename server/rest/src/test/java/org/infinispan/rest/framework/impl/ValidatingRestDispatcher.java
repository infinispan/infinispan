package org.infinispan.rest.framework.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

public class ValidatingRestDispatcher implements RestDispatcher {
   private final RestDispatcher delegate;

   public ValidatingRestDispatcher(RestDispatcher delegate) {
      this.delegate = delegate;
   }

   @Override
   public void initialize() {
      delegate.initialize();
   }

   @Override
   public LookupResult lookupInvocation(RestRequest restRequest) {
      return delegate.lookupInvocation(restRequest);
   }

   @Override
   public CompletionStage<RestResponse> dispatch(RestRequest restRequest) {
      return dispatch(restRequest, lookupInvocation(restRequest));
   }

   @Override
   public CompletionStage<RestResponse> dispatch(RestRequest restRequest, LookupResult lookupResult) {
      Invocation invocation = lookupResult.getInvocation();
      if (invocation != null && restRequest.path().startsWith("/rest/v3/")) {
         restRequest = new ValidatingRestRequest(restRequest, invocation);
      }
      return delegate.dispatch(restRequest, lookupResult);
   }
}
