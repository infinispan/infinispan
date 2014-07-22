package org.infinispan.server.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.infinispan.Cache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.websocket.json.JsonConversionException;
import org.infinispan.server.websocket.json.JsonObject;

import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Web Socket Server Handler (Netty).
 * <p>
 *    Websocket specific code lifted from Netty WebSocket Server example.
 * </p>
 */
public class WebSocketServerHandler extends SimpleChannelInboundHandler<Object> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private static final String INFINISPAN_WS_JS_FILENAME = "infinispan-ws.js";
   private CacheContainer cacheContainer;
   private Map<String, OpHandler> operationHandlers;
   private boolean connectionUpgraded;
   private final Map<String, Cache<Object, Object>> startedCaches;
   private WebSocketServerHandshaker handshaker;

   public WebSocketServerHandler(CacheContainer cacheContainer, Map<String, OpHandler> operationHandlers, Map<String, Cache<Object, Object>> startedCaches) {
      this.cacheContainer = cacheContainer;
      this.operationHandlers = operationHandlers;
      this.startedCaches = startedCaches;
   }

   @Override
   public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof FullHttpRequest) {
         handleHttpRequest(ctx, (FullHttpRequest) msg);
      } else if (msg instanceof WebSocketFrame) {
         handleWebSocketFrame(ctx, (WebSocketFrame) msg);
      }
   }

   private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
      // Allow only GET methods.
      if (req.getMethod() != GET) {
         sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN, Unpooled.EMPTY_BUFFER));
         return;
      }

      if (!connectionUpgraded && req.getUri().equalsIgnoreCase("/" + INFINISPAN_WS_JS_FILENAME)) {
         DefaultFullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK);
         loadScriptToResponse(req, res);
         sendHttpResponse(ctx, req, res);
      } else {
         // Handshake
         WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
               getWebSocketLocation(req), null, false);
         handshaker = wsFactory.newHandshaker(req);
         // Check if we can find the right handshaker for the requested version
         if (handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
         } else {
            handshaker.handshake(ctx.channel(), req).addListener(new ChannelFutureListener() {
               @Override
               public void operationComplete(ChannelFuture future) throws Exception {
                  if (!future.isSuccess()) {
                     // Handshake failed with an Exception, forward it to the other handlers in the chain
                     future.channel().pipeline().fireExceptionCaught(future.cause());
                  } else {
                     connectionUpgraded = true;
                  }
               }
            });
         }
      }
   }

   private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
      if (frame instanceof PingWebSocketFrame) {
         // received a ping, so write back a pong
         ctx.channel().writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
      } else if (frame instanceof CloseWebSocketFrame) {
         // request to close the connection
         handshaker.close(ctx.channel(), ((CloseWebSocketFrame) frame).retain());
      } else {
         try {
            ByteBuf binaryData = frame.content();
            if (binaryData == null) {
               return;
            }
            JsonObject payload = JsonObject.fromString(binaryData.toString(CharsetUtil.UTF_8));
            String opCode = (String) payload.get(OpHandler.OP_CODE);
            String cacheName = (String) payload.get(OpHandler.CACHE_NAME);
            Cache<Object, Object> cache = getCache(cacheName);

            OpHandler handler = operationHandlers.get(opCode);
            if (handler != null) {
               handler.handleOp(payload, cache, ctx);
            }
         } catch (JsonConversionException e) {
            log.error("Could not handle Web Socket Frame, error while converting to JSON", e);
         }
      }
   }

   private Cache<Object, Object> getCache(final String cacheName) {
      String key = cacheName;
      Cache<Object, Object> cache;

      if (key == null) {
         key = "";
      }

      cache = startedCaches.get(key);

      if (cache == null) {
         synchronized (startedCaches) {
            cache = startedCaches.get(key);
            if (cache == null) {
               if (cacheName != null) {
                  cache = cacheContainer.getCache(key);
               } else {
                  cache = cacheContainer.getCache();
               }
               startedCaches.put(key, cache);
               cache.start();
            }
         }
      }

      return cache;
   }

   private void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
      // Generate an error page if response status code is not OK (200).
      if (res.getStatus().code() != 200) {
         res.content().writeBytes(res.getStatus().toString().getBytes(CharsetUtil.UTF_8));
         HttpHeaders.setContentLength(res, res.content().readableBytes());
      }

      // Send the response and close the connection if necessary.
      ChannelFuture f = ctx.channel().writeAndFlush(res);
      if (!isKeepAlive(req) || res.getStatus().code() != 200) {
         f.addListener(ChannelFutureListener.CLOSE);
      }
   }

   private void loadScriptToResponse(FullHttpRequest req, DefaultFullHttpResponse res) {
      String wsAddress = getWebSocketLocation(req);

      StringWriter writer = new StringWriter();
      writer.write("var defaultWSAddress = '" + wsAddress + "';");
      writer.write(WebSocketServer.getJavascript());

      ByteBuf content = res.content().writeBytes(writer.toString().getBytes(CharsetUtil.UTF_8));

      res.headers().set(CONTENT_TYPE, "text/javascript; charset=UTF-8");
      setContentLength(res, content.readableBytes());
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      log.debugf(cause, "Error processing request on channel %s" , ctx.name());
      ctx.close();
   }

   private String getWebSocketLocation(HttpRequest req) {
      return "ws://" + req.headers().get(HttpHeaders.Names.HOST) + "/";
   }
}