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
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.StringWriter;
import java.security.MessageDigest;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;
import org.jboss.netty.handler.codec.http.HttpHeaders.Values;
import org.jboss.netty.handler.codec.http.websocket.WebSocketFrame;
import org.jboss.netty.handler.codec.http.websocket.WebSocketFrameDecoder;
import org.jboss.netty.handler.codec.http.websocket.WebSocketFrameEncoder;
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
      } else if (Values.UPGRADE.equalsIgnoreCase(req.getHeader(CONNECTION)) &&
            WEBSOCKET.equalsIgnoreCase(req.getHeader(Names.UPGRADE))) {
         // Serve the WebSocket handshake request.
         // Create the WebSocket handshake response.
         HttpResponse res = new DefaultHttpResponse(HTTP_1_1, new HttpResponseStatus(101, "Web Socket Protocol Handshake"));
         res.addHeader(Names.UPGRADE, Values.WEBSOCKET);
         res.addHeader(Names.CONNECTION, Values.UPGRADE);

         // Fill in the headers and contents depending on handshake method.
         if (req.containsHeader(Names.SEC_WEBSOCKET_KEY1) &&
            req.containsHeader(Names.SEC_WEBSOCKET_KEY2)) {
            // New handshake method with a challenge:
            res.addHeader(Names.SEC_WEBSOCKET_ORIGIN, req.getHeader(Names.ORIGIN));
            res.addHeader(Names.SEC_WEBSOCKET_LOCATION, getWebSocketLocation(req));
            String protocol = req.getHeader(Names.SEC_WEBSOCKET_PROTOCOL);
            if (protocol != null) {
               res.addHeader(Names.SEC_WEBSOCKET_PROTOCOL, protocol);
            }

            // Calculate the answer of the challenge.
            String key1 = req.getHeader(Names.SEC_WEBSOCKET_KEY1);
            String key2 = req.getHeader(Names.SEC_WEBSOCKET_KEY2);
            int a = (int) (Long.parseLong(key1.replaceAll("[^0-9]", "")) / key1.replaceAll("[^ ]", "").length());
            int b = (int) (Long.parseLong(key2.replaceAll("[^0-9]", "")) / key2.replaceAll("[^ ]", "").length());
            long c = req.getContent().readLong();
            ChannelBuffer input = ChannelBuffers.buffer(16);
            input.writeInt(a);
            input.writeInt(b);
            input.writeLong(c);
            ChannelBuffer output = ChannelBuffers.wrappedBuffer(
                    MessageDigest.getInstance("MD5").digest(input.array()));
            res.setContent(output);
         } else {
            // Old handshake method with no challenge:
            res.addHeader(Names.WEBSOCKET_ORIGIN, req.getHeader(Names.ORIGIN));
            res.addHeader(Names.WEBSOCKET_LOCATION, getWebSocketLocation(req));
            String protocol = req.getHeader(Names.WEBSOCKET_PROTOCOL);
            if (protocol != null) {
               res.addHeader(Names.WEBSOCKET_PROTOCOL, protocol);
            }
         }

         // Upgrade the connection and send the handshake response.
         ChannelPipeline p = ctx.getChannel().getPipeline();
         p.remove("aggregator");
         p.replace("decoder", "wsdecoder", new WebSocketFrameDecoder());

         ctx.getChannel().write(res);

         p.replace("encoder", "wsencoder", new WebSocketFrameEncoder());
         return;
      }

      // Send an error page otherwise.
      sendHttpResponse(ctx, req, new DefaultHttpResponse(HTTP_1_1, FORBIDDEN));
   }

   private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
      try {
         JSONObject payload = new JSONObject(frame.getTextData());
         String opCode = (String) payload.get(OpHandler.OP_CODE);
         String cacheName = (String) payload.opt(OpHandler.CACHE_NAME);
         Cache<Object, Object> cache = getCache(cacheName);

         OpHandler handler = operationHandlers.get(opCode);
         if (handler != null) {
            handler.handleOp(payload, cache, ctx);
         }
      } catch (JSONException e) {
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