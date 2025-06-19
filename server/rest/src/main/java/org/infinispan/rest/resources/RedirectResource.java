package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.GET;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST resource to serve redirects
 *
 * @since 10.1
 */
public class RedirectResource implements ResourceHandler {

   private final String path;
   private final String redirectPath;
   private final boolean anonymous;
   private final InvocationHelper invocationHelper;

   public RedirectResource(InvocationHelper invocationHelper, String path, String redirectPath, boolean anonymous) {
      this.invocationHelper = invocationHelper;
      this.path = path;
      this.redirectPath = redirectPath;
      this.anonymous = anonymous;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("redirect", "REST resource to redirect requests.")
            .invocation().methods(GET).path(path).anonymous(anonymous).handleWith(this::redirect)
            .create();
   }

   private CompletionStage<RestResponse> redirect(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      responseBuilder.status(HttpResponseStatus.TEMPORARY_REDIRECT).header("Location", redirectPath);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }
}
