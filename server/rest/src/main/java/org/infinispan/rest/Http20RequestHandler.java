package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.unix.Errors;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;

/**
 * Netty REST handler for HTTP/2.0
 *
 * @author Sebastian Łaskawiec
 */
public class Http20RequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

   protected final static Log logger = LogFactory.getLog(Http20RequestHandler.class, Log.class);

   private final Authenticator authenticator;
   final RestAccessLoggingHandler restAccessLoggingHandler = new RestAccessLoggingHandler();
   protected final RestServer restServer;
   protected final RestServerConfiguration configuration;

   /**
    * Creates new {@link Http20RequestHandler}.
    *
    * @param restServer    Rest Server.
    */
   public Http20RequestHandler(RestServer restServer) {
      this.restServer = restServer;
      this.configuration = restServer.getConfiguration();
      this.authenticator = restServer.getAuthenticator();
   }

   @Override
   public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
      restAccessLoggingHandler.preLog(request);
      NettyRestRequest restRequest = new NettyRestRequest(request);
      authenticator.challenge(restRequest, ctx).whenComplete((authResponse, authThrowable) -> {
         if (authThrowable != null) {
            restRequest.release();
            handleError(ctx, request, authThrowable);
         } else if (authResponse.getStatus() == UNAUTHORIZED.code()) {
            restRequest.release();
            sendResponse(ctx, request, ((NettyRestResponse) authResponse).getResponse());
         } else {
            restServer.getRestDispatcher().dispatch(restRequest).whenComplete((restResponse, throwable) -> {
               restRequest.release();
               if (throwable == null) {
                  NettyRestResponse nettyRestResponse = (NettyRestResponse) restResponse;
                  addCorrelatedHeaders(request, nettyRestResponse.getResponse());
                  sendResponse(ctx, request, nettyRestResponse.getResponse());
               } else {
                  handleError(ctx, request, throwable);
               }
            });
         }
      });
   }

   private void handleError(ChannelHandlerContext ctx, FullHttpRequest request, Throwable throwable) {
      Throwable cause = CompletableFutures.extractException(throwable);
      NettyRestResponse errorResponse;
      if (cause instanceof RestResponseException) {
         RestResponseException responseException = (RestResponseException) throwable;
         logger.errorWhileResponding(responseException);
         errorResponse = new NettyRestResponse.Builder().status(responseException.getStatus()).entity(responseException.getText()).build();
      } else if (cause instanceof SecurityException) {
         errorResponse = new NettyRestResponse.Builder().status(FORBIDDEN).entity(cause.getMessage()).build();
      } else if (cause instanceof CacheConfigurationException) {
         errorResponse = new NettyRestResponse.Builder().status(BAD_REQUEST).entity(cause.getMessage()).build();
      } else {
         errorResponse = new NettyRestResponse.Builder().status(INTERNAL_SERVER_ERROR).entity(cause.getMessage()).build();
      }
      sendResponse(ctx, request, errorResponse.getResponse());
   }

   private void addCorrelatedHeaders(FullHttpRequest request, FullHttpResponse response) {
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

   protected void sendResponse(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
      ctx.executor().execute(() -> {
         restAccessLoggingHandler.log(ctx, request, response);
         ctx.writeAndFlush(response);
      });
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
