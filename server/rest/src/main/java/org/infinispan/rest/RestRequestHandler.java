package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Objects;

import javax.security.auth.Subject;

import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.Errors;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;

/**
 * Netty handler for REST requests.
 *
 * @author Sebastian Łaskawiec
 */
public class RestRequestHandler extends BaseHttpRequestHandler {

   protected final static Log logger = LogFactory.getLog(RestRequestHandler.class, Log.class);
   protected final RestServer restServer;
   protected final RestServerConfiguration configuration;
   private final String context;
   private Subject subject;
   private String authorization;
   private final Authenticator authenticator;

   /**
    * Creates new {@link RestRequestHandler}.
    *
    * @param restServer Rest Server.
    */
   RestRequestHandler(RestServer restServer) {
      this.restServer = restServer;
      this.configuration = restServer.getConfiguration();
      this.authenticator = configuration.authentication().enabled() ? configuration.authentication().authenticator() : null;
      this.context = configuration.contextPath();
   }

   @Override
   public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
      restAccessLoggingHandler.preLog(request);
      if (!Method.contains(request.getMethod().name())) {
         NettyRestResponse restResponse = new NettyRestResponse.Builder().status(FORBIDDEN).build();
         sendResponse(ctx, request, restResponse);
         return;
      }

      NettyRestRequest restRequest;
      LookupResult invocationLookup;
      try {
         restRequest = new NettyRestRequest(request);
         invocationLookup = restServer.getRestDispatcher().lookupInvocation(restRequest);
      } catch (Exception e) {
         NettyRestResponse restResponse = new NettyRestResponse.Builder().status(BAD_REQUEST).build();
         sendResponse(ctx, request, restResponse);
         return;
      }

      if (authenticator == null || isAnon(invocationLookup)) {
         handleRestRequest(ctx, restRequest, invocationLookup);
         return;
      }
      if (subject != null) {
         // Ensure that the authorization header, if needed, has not changed
         String authz = request.headers().get(HttpHeaderNames.AUTHORIZATION);
         if (Objects.equals(authz, authorization)) {
            handleRestRequest(ctx, restRequest, invocationLookup);
            return;
         } else {
            // Invalidate and force re-authentication
            subject = null;
            authorization = null;
         }
      }
      authenticator.challenge(restRequest, ctx).whenComplete((authResponse, authThrowable) -> {
         boolean hasError = authThrowable != null;
         boolean authorized = authResponse.getStatus() != UNAUTHORIZED.code();
         if (!hasError && authorized) {
            subject = restRequest.getSubject();
            authorization = restRequest.getAuthorizationHeader();
            restRequest.setSubject(subject);
            handleRestRequest(ctx, restRequest, invocationLookup);
         } else {
            try {
               if (hasError) {
                  handleError(ctx, request, authThrowable);
               } else {
                  sendResponse(ctx, request, ((NettyRestResponse) authResponse));
               }
            } finally {
               request.release();
            }
         }
      });
   }

   private boolean isAnon(LookupResult lookupResult) {
      if (lookupResult == null || lookupResult.getInvocation() == null) return true;
      return lookupResult.getInvocation().anonymous();
   }

   private void handleRestRequest(ChannelHandlerContext ctx, NettyRestRequest restRequest, LookupResult invocationLookup) {
      restServer.getRestDispatcher().dispatch(restRequest, invocationLookup).whenComplete((restResponse, throwable) -> {
         FullHttpRequest request = restRequest.getFullHttpRequest();
         try {
            if (throwable == null) {
               NettyRestResponse nettyRestResponse = (NettyRestResponse) restResponse;
               addCorrelatedHeaders(request, nettyRestResponse.getResponse());
               sendResponse(ctx, request, nettyRestResponse);
            } else {
               handleError(ctx, request, throwable);
            }
         } finally {
            request.release();
         }
      });
   }

   private void addCorrelatedHeaders(FullHttpRequest request, HttpResponse response) {
      String streamId = request.headers().get(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
      if (streamId != null) {
         response.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
      }
      boolean isKeepAlive = HttpUtil.isKeepAlive(request);
      HttpVersion httpVersion = response.protocolVersion();
      if ((httpVersion == HttpVersion.HTTP_1_1 || httpVersion == HttpVersion.HTTP_1_0) && isKeepAlive) {
         response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }
   }

   @Override
   protected Log getLogger() {
      return logger;
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      // handle the case of to big requests.
      if (e.getCause() instanceof TooLongFrameException) {
         DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, REQUEST_ENTITY_TOO_LARGE);
         ctx.write(response).addListener(ChannelFutureListener.CLOSE);
      } else if (e instanceof Errors.NativeIoException) {
         // Native IO exceptions happen on HAProxy disconnect. It sends RST instead of FIN, which cases
         // a Netty IO Exception. The only solution is to ignore it, just like Tomcat does.
         logger.debug("Native IO Exception", e);
         ctx.close();
      } else {
         logger.uncaughtExceptionInThePipeline(e);
         ctx.close();
      }
   }
}
