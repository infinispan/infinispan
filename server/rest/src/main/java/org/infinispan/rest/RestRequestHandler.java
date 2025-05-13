package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.infinispan.commons.util.Util.unwrapExceptionMessage;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import javax.security.auth.Subject;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.remoting.RemoteException;
import org.infinispan.rest.authentication.RestAuthenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.topology.MissingMembersException;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.unix.Errors;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;

/**
 * Netty handler for REST requests.
 *
 * @author Sebastian Łaskawiec
 */
public class RestRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

   protected static final Log logger = LogFactory.getLog(RestRequestHandler.class, Log.class);
   private final RestAccessLoggingHandler restAccessLoggingHandler = new RestAccessLoggingHandler();
   protected final RestServer restServer;
   protected final RestServerConfiguration configuration;
   private Subject subject;
   private String authorization;
   private final RestAuthenticator authenticator;

   /**
    * Creates new {@link RestRequestHandler}.
    *
    * @param restServer Rest Server.
    */
   RestRequestHandler(RestServer restServer) {
      super(false);
      this.restServer = restServer;
      this.configuration = restServer.getConfiguration();
      this.authenticator = configuration.authentication().enabled() ? configuration.authentication().authenticator() : null;
   }

   void handleError(ChannelHandlerContext ctx, FullHttpRequest request, Throwable throwable) {
      Throwable cause = filterCause(throwable);
      NettyRestResponse.Builder errorResponse = restServer.getInvocationHelper().newResponse(request);
      if (cause instanceof RestResponseException) {
         RestResponseException responseException = (RestResponseException) throwable;
         if (getLogger().isTraceEnabled()) getLogger().tracef("Request failed: %s", responseException);
         errorResponse.status(responseException.getStatus()).entity(responseException.getText());
      } else if (cause instanceof SecurityException) {
         if (getLogger().isTraceEnabled()) getLogger().tracef("Request failed: %s", cause);
         errorResponse.status(FORBIDDEN).entity(unwrapExceptionMessage(cause));
      } else if (cause instanceof NoSuchElementException) {
         if (getLogger().isTraceEnabled()) getLogger().tracef("Request failed: %s", cause);
         errorResponse.status(NOT_FOUND).entity(unwrapExceptionMessage(cause));
      } else if (cause instanceof CacheConfigurationException || cause instanceof IllegalArgumentException || cause instanceof EncodingException || cause instanceof Json.MalformedJsonException || cause instanceof MissingMembersException) {
         if (getLogger().isTraceEnabled()) getLogger().tracef("Request failed: %s", cause);
         errorResponse.status(BAD_REQUEST).entity(unwrapExceptionMessage(cause));
      } else {
         getLogger().errorWhileResponding(throwable);
         errorResponse.status(INTERNAL_SERVER_ERROR).entity(unwrapExceptionMessage(cause));
      }

      if (HttpMethod.HEAD.equals(request.method()))
         errorResponse.entity(null);

      sendResponse(ctx, request, errorResponse.build());
   }

   public static Throwable filterCause(Throwable re) {
      if (re == null) return null;
      Class<? extends Throwable> tClass = re.getClass();
      Throwable cause = re.getCause();
      if (cause != null && (tClass == ExecutionException.class || tClass == CompletionException.class || tClass == InvocationTargetException.class || tClass == RemoteException.class || tClass == RuntimeException.class || tClass == CacheListenerException.class))
         return filterCause(cause);
      else
         return re;
   }

   void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, NettyRestResponse response) {
      ctx.executor().execute(() -> ResponseWriter.forContent(request, response.getEntity()).writeResponse(ctx, request, response));
   }

   @Override
   public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
      if (logger.isTraceEnabled()) {
         logger.trace(HttpMessageUtil.dumpRequest(request));
      }

      restAccessLoggingHandler.preLog(request);
      if (HttpUtil.is100ContinueExpected(request)) {
         ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
      }
      if (!Method.contains(request.method().name())) {
         NettyRestResponse restResponse = new NettyRestResponse.Builder().status(FORBIDDEN).build();
         sendResponse(ctx, request, restResponse);
         return;
      }

      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      metadata.protocolVersion(request.protocolVersion().text());
      String userAgent = request.headers().get(HttpHeaderNames.USER_AGENT);
      if (userAgent != null) {
         metadata.clientLibraryName(userAgent);
      }
      NettyRestRequest restRequest;
      LookupResult invocationLookup;
      try {
         restRequest = new NettyRestRequest(request, (InetSocketAddress) ctx.channel().remoteAddress());
         invocationLookup = restServer.getRestDispatcher().lookupInvocation(restRequest);
         Invocation invocation = invocationLookup.getInvocation();
         if (invocation != null && invocation.deprecated()) {
            logger.warnDeprecatedCall(restRequest.toString());
         }
      } catch (Exception e) {
         if (logger.isDebugEnabled()) {
            logger.debug("Error during REST dispatch", e);
         }
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
            if (logger.isTraceEnabled()) {
               logger.tracef("Authorization header match, skipping authentication for %s", request);
            }
            restRequest.setSubject(subject);
            handleRestRequest(ctx, restRequest, invocationLookup);
            return;
         } else {
            // Invalidate and force re-authentication
            if (logger.isTraceEnabled()) {
               logger.tracef("Authorization header mismatch:\n%s\n%s", authz, authorization);
            }
            subject = null;
            authorization = null;
         }
      }
      authenticator.challenge(restRequest, ctx).whenComplete((authResponse, authThrowable) -> {
         boolean hasError = authThrowable != null;
         boolean authorized = !hasError && authResponse.getStatus() < BAD_REQUEST.code();
         if (authorized) {
            authorization = restRequest.getAuthorizationHeader();
            subject = restRequest.getSubject();
            metadata.subject(subject);
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
               sendResponse(ctx, request, nettyRestResponse);
            } else {
               handleError(ctx, request, throwable);
            }
         } finally {
            request.release();
         }
      });
   }

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
         logger.debugf(e, "Native IO Exception from %s", ctx.channel().remoteAddress());
         ctx.close();
      } else if (!ctx.channel().isActive() && e instanceof IllegalStateException &&
            e.getMessage().equals("ssl is null")) {
         // Workaround for ISPN-12558 -- OpenSSLEngine shut itself down too soon
         // Ignore the exception, trying to close the context will cause a StackOverflowError
      } else {
         logger.uncaughtExceptionInThePipeline(e);
         ctx.close();
      }
   }
}
