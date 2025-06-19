package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.server.core.ProtocolServer;

import io.netty.handler.codec.http.HttpHeaderNames;

public class SwaggerUIResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;


   public SwaggerUIResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("swaggerui", "Swagger UI Initializer")
            .invocation().methods(Method.GET).path("/swagger-initializer").anonymous(true)
            .response(OK, "", MediaType.TEXT_JAVASCRIPT)
            .handleWith(this::swaggerInitializer)
            .create();
   }

   private CompletionStage<RestResponse> swaggerInitializer(RestRequest request) {
      ProtocolServer<?> server = invocationHelper.getProtocolServer().getEnclosingProtocolServer();
      if (server == null) {
         server = invocationHelper.getProtocolServer();
      }
      String js = String.format("""
            window.onload = function() {
              window.ui = SwaggerUIBundle({
                url: "%s://%s/%s/v3/openapi",
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                  SwaggerUIBundle.presets.apis
                ]
              });
            };
            """, server.getConfiguration().ssl().enabled() ? "https" : "http", request.header(HttpHeaderNames.HOST.toString()), invocationHelper.getContext());
      return CompletableFuture.completedFuture(invocationHelper.newResponse(request)
            .status(OK)
            .entity(js).contentType(MediaType.TEXT_JAVASCRIPT)
            .build());
   }
}
