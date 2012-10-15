/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.websocket;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.StringWriter;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import org.jboss.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import org.jboss.netty.util.CharsetUtil;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Web Socket Server Handler (Netty).
 * <p/>
 * Websocket specific code lifted from Netty WebSocket Server example.
 */
public class WebSocketServerHandler extends SimpleChannelUpstreamHandler {

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
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      Object msg = e.getMessage();
      if (msg instanceof HttpRequest) {
         handleHttpRequest(ctx, (HttpRequest) msg);
      } else if (msg instanceof WebSocketFrame) {
         handleWebSocketFrame(ctx, (WebSocketFrame) msg);
      }
   }

   private void handleHttpRequest(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
      // Allow only GET methods.
      if (req.getMethod() != GET) {
         sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
         return;
      }

      if (!connectionUpgraded && req.getUri().equalsIgnoreCase("/" + INFINISPAN_WS_JS_FILENAME)) {
         DefaultHttpResponse res = new DefaultHttpResponse(HTTP_1_1, OK);
         loadScriptToResponse(req, res);
         sendHttpResponse(ctx, req, res);
         return;
      } else {
          // Handshake
          WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                  getWebSocketLocation(req), null, false);
          handshaker = wsFactory.newHandshaker(req);
          // Check if we can find the right handshaker for the requested version
          if (handshaker == null) {
              wsFactory.sendUnsupportedWebSocketVersionResponse(ctx.getChannel());
          } else {
              // fuehre den Handshake
              handshaker.handshake(ctx.getChannel(), req).addListener(new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                      if(!future.isSuccess()) {
                          // Handshake failed with an Exception, forward it to the other handlers in the chain
                          Channels.fireExceptionCaught(future.getChannel(), future.getCause());
                      } else {
                         connectionUpgraded = true;
                      }
                  }
              });
          }

         return;
      }
   }

   private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
      if (frame instanceof PingWebSocketFrame) {
         // received a ping, so write back a pong
         ctx.getChannel().write(new PongWebSocketFrame(frame.getBinaryData()));
      } else if (frame instanceof CloseWebSocketFrame) {
         // request to close the connection
         handshaker.close(ctx.getChannel(), (CloseWebSocketFrame) frame);
      } else {
         try {
            ChannelBuffer binaryData = frame.getBinaryData();
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

   private void sendHttpResponse(ChannelHandlerContext ctx, HttpRequest req, HttpResponse res) {
      // Generate an error page if response status code is not OK (200).
      if (res.getStatus().getCode() != 200) {
         res.setContent(ChannelBuffers.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8));
         HttpHeaders.setContentLength(res, res.getContent().readableBytes());
      }

      // Send the response and close the connection if necessary.
      ChannelFuture f = ctx.getChannel().write(res);
      if (!isKeepAlive(req) || res.getStatus().getCode() != 200) {
         f.addListener(ChannelFutureListener.CLOSE);
      }
   }

   private void loadScriptToResponse(HttpRequest req, DefaultHttpResponse res) {
      String wsAddress = getWebSocketLocation(req);

      StringWriter writer = new StringWriter();
      writer.write("var defaultWSAddress = '" + wsAddress + "';");
      writer.write(WebSocketServer.getJavascript());

      ChannelBuffer content = ChannelBuffers.copiedBuffer(writer.toString(), CharsetUtil.UTF_8);

      res.setHeader(CONTENT_TYPE, "text/javascript; charset=UTF-8");
      setContentLength(res, content.readableBytes());
      res.setContent(content);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      e.getCause().printStackTrace();
      e.getChannel().close();
   }

   private String getWebSocketLocation(HttpRequest req) {
      return "ws://" + req.getHeader(HttpHeaders.Names.HOST) + "/";
   }
}