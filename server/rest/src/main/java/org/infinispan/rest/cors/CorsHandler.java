package org.infinispan.rest.cors;

import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.util.ReferenceCountUtil.release;

import java.util.List;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * Handles <a href="http://www.w3.org/TR/cors/">Cross Origin Resource Sharing</a> (CORS) requests.
 * <p>
 * This handler can be configured using one or more {@link CorsConfig}, please
 * refer to this class for details about the configuration options available.
 *
 * //TODO: remove when multi-config CorsHandler is supported by Netty (https://github.com/netty/netty/issues/7785)
 */
public class CorsHandler extends ChannelDuplexHandler {

   private static final InternalLogger logger = InternalLoggerFactory.getInstance(io.netty.handler.codec.http.cors.CorsHandler.class);
   private static final String ANY_ORIGIN = "*";
   private static final String NULL_ORIGIN = "null";
   private CorsConfig config;

   private HttpRequest request;
   private final List<CorsConfig> configList;
   private boolean isShortCircuit;

   /**
    * Creates a new instance with the specified config list. If more than one
    * config matches a certain origin, the first in the List will be used.
    *
    * @param configList     List of {@link CorsConfig}
    * @param isShortCircuit Same as {@link CorsConfig#shortCircuit} but applicable to all supplied configs.
    */
   public CorsHandler(final List<CorsConfig> configList, boolean isShortCircuit) {
      if (configList == null || configList.size() < 1) {
         throw new IllegalArgumentException("List of corsConfig must contain at least one item");
      }
      this.configList = configList;
      this.isShortCircuit = isShortCircuit;
   }

   @Override
   public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
      if (msg instanceof HttpRequest) {
         request = (HttpRequest) msg;
         final String origin = request.headers().get(HttpHeaderNames.ORIGIN);
         config = getForOrigin(origin);
         if (isPreflightRequest(request)) {
            handlePreflight(ctx, request);
            return;
         }
         if (isShortCircuit && !(origin == null || config != null)) {
            forbidden(ctx, request);
            return;
         }
      }
      ctx.fireChannelRead(msg);
   }

   private void handlePreflight(final ChannelHandlerContext ctx, final HttpRequest request) {
      final HttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), OK, true, true);
      if (setOrigin(response)) {
         setAllowMethods(response);
         setAllowHeaders(response);
         setAllowCredentials(response);
         setMaxAge(response);
         setPreflightHeaders(response);
      }
      if (!response.headers().contains(HttpHeaderNames.CONTENT_LENGTH)) {
         response.headers().set(HttpHeaderNames.CONTENT_LENGTH, HttpHeaderValues.ZERO);
      }
      release(request);
      respond(ctx, request, response);
   }

   /**
    * This is a non CORS specification feature which enables the setting of preflight
    * response headers that might be required by intermediaries.
    *
    * @param response the HttpResponse to which the preflight response headers should be added.
    */
   private void setPreflightHeaders(final HttpResponse response) {
      response.headers().add(config.preflightResponseHeaders());
   }

   private CorsConfig getForOrigin(String requestOrigin) {
      for (CorsConfig corsConfig : configList) {
         if (corsConfig.isAnyOriginSupported()) {
            return corsConfig;
         }
         if (corsConfig.origins().contains(requestOrigin)) {
            return corsConfig;
         }
         if (corsConfig.isNullOriginAllowed() || NULL_ORIGIN.equals(requestOrigin)) {
            return corsConfig;
         }
      }
      return null;
   }

   private boolean setOrigin(final HttpResponse response) {
      final String origin = request.headers().get(HttpHeaderNames.ORIGIN);
      if (origin != null && config != null) {
         if (NULL_ORIGIN.equals(origin) && config.isNullOriginAllowed()) {
            setNullOrigin(response);
            return true;
         }
         if (config.isAnyOriginSupported()) {
            if (config.isCredentialsAllowed()) {
               echoRequestOrigin(response);
               setVaryHeader(response);
            } else {
               setAnyOrigin(response);
            }
            return true;
         }
         if (config.origins().contains(origin)) {
            setOrigin(response, origin);
            setVaryHeader(response);
            return true;
         }
         logger.debug("Request origin [{}]] was not among the configured origins [{}]", origin, config.origins());
      }
      return false;
   }

   private void echoRequestOrigin(final HttpResponse response) {
      setOrigin(response, request.headers().get(HttpHeaderNames.ORIGIN));
   }

   private static void setVaryHeader(final HttpResponse response) {
      response.headers().set(HttpHeaderNames.VARY, HttpHeaderNames.ORIGIN);
   }

   private static void setAnyOrigin(final HttpResponse response) {
      setOrigin(response, ANY_ORIGIN);
   }

   private static void setNullOrigin(final HttpResponse response) {
      setOrigin(response, NULL_ORIGIN);
   }

   private static void setOrigin(final HttpResponse response, final String origin) {
      response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
   }

   private void setAllowCredentials(final HttpResponse response) {
      if (config.isCredentialsAllowed()
            && !response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN).equals(ANY_ORIGIN)) {
         response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
      }
   }

   private static boolean isPreflightRequest(final HttpRequest request) {
      final HttpHeaders headers = request.headers();
      return request.method().equals(OPTIONS) &&
            headers.contains(HttpHeaderNames.ORIGIN) &&
            headers.contains(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD);
   }

   private void setExposeHeaders(final HttpResponse response) {
      if (!config.exposedHeaders().isEmpty()) {
         response.headers().set(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, config.exposedHeaders());
      }
   }

   private void setAllowMethods(final HttpResponse response) {
      response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, config.allowedRequestMethods());
   }

   private void setAllowHeaders(final HttpResponse response) {
      response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, config.allowedRequestHeaders());
   }

   private void setMaxAge(final HttpResponse response) {
      response.headers().set(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, config.maxAge());
   }

   @Override
   public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
      if (config != null && config.isCorsSupportEnabled() && msg instanceof HttpResponse) {
         final HttpResponse response = (HttpResponse) msg;
         if (setOrigin(response)) {
            setAllowCredentials(response);
            setExposeHeaders(response);
         }
      }
      ctx.writeAndFlush(msg, promise);
   }

   private static void forbidden(final ChannelHandlerContext ctx, final HttpRequest request) {
      HttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(), FORBIDDEN);
      response.headers().set(HttpHeaderNames.CONTENT_LENGTH, HttpHeaderValues.ZERO);
      release(request);
      respond(ctx, request, response);
   }

   private static void respond(
         final ChannelHandlerContext ctx,
         final HttpRequest request,
         final HttpResponse response) {

      final boolean keepAlive = HttpUtil.isKeepAlive(request);

      HttpUtil.setKeepAlive(response, keepAlive);

      final ChannelFuture future = ctx.writeAndFlush(response);
      if (!keepAlive) {
         future.addListener(ChannelFutureListener.CLOSE);
      }
   }
}
