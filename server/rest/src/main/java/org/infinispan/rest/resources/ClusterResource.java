package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.Security;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.0
 */
public class ClusterResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;

   public ClusterResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET, POST).path("/v2/cluster").withAction("stop").handleWith(this::stop)
            .create();
   }

   private CompletionStage<RestResponse> stop(RestRequest restRequest) {
      List<String> servers = restRequest.parameters().get("server");

      HttpResponseStatus status = restRequest.method().equals(POST) ? NO_CONTENT: OK;

      if (servers != null && !servers.isEmpty()) {
         Security.doAs(restRequest.getSubject(), () -> invocationHelper.getServer().serverStop(servers));
      } else {
         Security.doAs(restRequest.getSubject(), () -> invocationHelper.getServer().clusterStop());
      }

      return CompletableFuture.completedFuture(new NettyRestResponse.Builder().status(status).build());
   }
}
