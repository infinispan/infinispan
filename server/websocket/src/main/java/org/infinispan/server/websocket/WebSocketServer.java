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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.CustomReplayingDecoder;
import org.infinispan.server.websocket.handlers.GetHandler;
import org.infinispan.server.websocket.handlers.NotifyHandler;
import org.infinispan.server.websocket.handlers.PutHandler;
import org.infinispan.server.websocket.handlers.RemoveHandler;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * An HTTP server which serves Web Socket requests on an Infinispan cacheManager.
 * <p/>
 * Websocket specific code lifted from Netty WebSocket Server example.
 */
public class WebSocketServer extends AbstractProtocolServer {

   public static final String INFINISPAN_WS_JS_FILENAME = "infinispan-ws.js";

   private static String javascript;
   private Channel channel;

   public WebSocketServer() {
      super("WebSocketServerThread");
   }

   public OneToOneEncoder getEncoder() {
      return null;
   }

   public CustomReplayingDecoder getDecoder() {
      return null;
   }

//   @Override
   public void start(Properties p, EmbeddedCacheManager cacheManager) {
      Properties properties = p == null ? new Properties() : p;
      super.start(properties, cacheManager, 8181);
   }

   @Override
   public void startTransport(int idleTimeout, boolean tcpNoDelay, int sendBufSize, int recvBufSize, TypedProperties typedProps) {
      InetSocketAddress address = new InetSocketAddress(getHost(), getPort());
      Executor masterExecutor = Executors.newCachedThreadPool();
      Executor workerExecutor = Executors.newCachedThreadPool();

      NioServerSocketChannelFactory factory = new NioServerSocketChannelFactory(masterExecutor, workerExecutor, workerThreads());

      // Configure the server.
      ServerBootstrap bootstrap = new ServerBootstrap(factory);

      // Set up the event pipeline factory.
      bootstrap.setPipelineFactory(new WebSocketServerPipelineFactory(cacheManager()));

      // Bind and start to accept incoming connections.
      bootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
      if (sendBufSize > 0) bootstrap.setOption("child.sendBufferSize", sendBufSize);
      if (recvBufSize > 0) bootstrap.setOption("receiveBufferSize", recvBufSize);

      bootstrap.bind(address);
   }

   @Override
   public void stop() {
      if (channel != null) channel.close();
   }

   private static class WebSocketServerPipelineFactory implements ChannelPipelineFactory {

      private CacheContainer cacheContainer;
      private Map<String, OpHandler> operationHandlers;
      private Map<String, Cache> startedCaches = ConcurrentMapFactory.makeConcurrentMap();

      public WebSocketServerPipelineFactory(CacheContainer cacheContainer) {
         this.cacheContainer = cacheContainer;

         operationHandlers = new HashMap<String, OpHandler>();
         operationHandlers.put("put", new PutHandler());
         operationHandlers.put("get", new GetHandler());
         operationHandlers.put("remove", new RemoveHandler());
         NotifyHandler notifyHandler = new NotifyHandler();
         operationHandlers.put("notify", notifyHandler);
         operationHandlers.put("unnotify", notifyHandler);
      }

      @Override
      public ChannelPipeline getPipeline() throws Exception {
         // Create a default pipeline implementation.
         ChannelPipeline pipeline = Channels.pipeline();

         pipeline.addLast("decoder", new HttpRequestDecoder());
         pipeline.addLast("aggregator", new HttpChunkAggregator(65536));
         pipeline.addLast("encoder", new HttpResponseEncoder());
         pipeline.addLast("handler", new WebSocketServerHandler(cacheContainer, operationHandlers, startedCaches));

         return pipeline;
      }
   }

   public static String getJavascript() {
      if (javascript != null) {
         return javascript;
      }

      BufferedReader scriptReader = new BufferedReader(new InputStreamReader(WebSocketServer.class.getResourceAsStream(INFINISPAN_WS_JS_FILENAME)));

      try {
         StringWriter writer = new StringWriter();

         String line = scriptReader.readLine();
         while (line != null) {
            writer.write(line);
            writer.write('\n');
            line = scriptReader.readLine();
         }

         javascript = writer.toString();

         return javascript;
      } catch (IOException e) {
         throw new IllegalStateException("Unexpected exception while sending Websockets script to client.", e);
      } finally {
         try {
            scriptReader.close();
         } catch (IOException e) {
            throw new IllegalStateException("Unexpected exception while closing Websockets script to client.", e);
         }
      }
   }
}
