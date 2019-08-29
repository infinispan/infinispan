package org.infinispan.rest.framework;

import java.util.concurrent.CompletionStage;

/**
 * Routes a particular {@link RestRequest} to be executed by the correct {link @Invocation}, and produces the {@link RestResponse}.
 *
 * @since 10.0
 */
public interface RestDispatcher {

   LookupResult lookupInvocation(RestRequest restRequest);

   CompletionStage<RestResponse> dispatch(RestRequest restRequest);

   CompletionStage<RestResponse> dispatch(RestRequest restRequest, LookupResult lookupResult);

}
