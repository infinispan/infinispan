package org.infinispan.rest.framework;

/**
 * Routes a particular {@link RestRequest} to be executed by the correct {link @Invocation}, and produces the {@link RestResponse}.
 *
 * @since 10.0
 */
public interface RestDispatcher {

   RestResponse dispatch(RestRequest restRequest);

}
