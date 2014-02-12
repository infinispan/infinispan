package org.infinispan.server.websocket;

import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

import java.io.StringWriter;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.CharsetUtil;
import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.manager.CacheContainer;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Web Socket Server Handler (Netty).
 * <p/>
 * Websocket specific code lifted from Netty WebSocket Server example.
 */
public class WebSocketServerHandler extends SimpleChannelInboundHandler<Object> {

   private static final String INFINISPAN_WS_JS_FILENAME = "infinispan-ws.js";
   private CacheContainer cacheContainer;
   private Map<String, OpHandler> operationHandlers;
   private boolean connectionUpgraded;
   private Map<String, Cache> startedCaches;
   private WebSocketServerHandshaker handshaker;
   
   public WebSocketServerHandler(CacheContainer cacheContainer, Map<String, OpHandler> operationHandlers, Map<String, Cache> startedCaches) {
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

   private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
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
                      if(!future.isSuccess()) {
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
            JSONObject payload = new JSONObject(binaryData.toString(CharsetUtil.UTF_8));
            String opCode = (String) payload.get(OpHandler.OP_CODE);
            String cacheName = (String) payload.opt(OpHandler.CACHE_NAME);
            Cache<Object, Object> cache = getCache(cacheName);
            
            OpHandler handler = operationHandlers.get(opCode);
            if (handler != null) {
               handler.handleOp(payload, cache, ctx);
            }
          } catch (JSONException e) {
             // ignore
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
      cause.printStackTrace();
      ctx.close();
   }

   private String getWebSocketLocation(HttpRequest req) {
      return "ws://" + req.headers().get(HttpHeaders.Names.HOST) + "/";
   }
}