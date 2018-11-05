package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
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
import io.netty.handler.codec.http.HttpResponseStatus;
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
      RestResponse response;
      NettyRestRequest restRequest = new NettyRestRequest(request);
      try {
         restAccessLoggingHandler.preLog(request);
         authenticator.challenge(restRequest, ctx);
         response = restServer.getRestDispatcher().dispatch(restRequest);
         if (response == null) {
            response = new NettyRestResponse.Builder().status(HttpResponseStatus.NOT_FOUND).build();
         }
         addCorrelatedHeaders(request, ((NettyRestResponse) response).getResponse());
      } catch (AuthenticationException authException) {
         response = new NettyRestResponse.Builder()
               .status(HttpResponseStatus.UNAUTHORIZED)
               .authenticate(authException.getAuthenticationHeader())
               .build();

      } catch (RestResponseException responseException) {
         logger.errorWhileResponding(responseException);
         response = new NettyRestResponse.Builder().status(responseException.getStatus()).entity(responseException.getText()).build();
      }

      NettyRestResponse nettyRestResponse = (NettyRestResponse) response;
      sendResponse(ctx, request, nettyRestResponse.getResponse());
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
