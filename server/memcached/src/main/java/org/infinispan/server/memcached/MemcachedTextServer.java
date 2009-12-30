/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.core.ServerBootstrap;
import org.infinispan.server.core.netty.NettyChannelPipelineFactory;
import org.infinispan.server.core.netty.NettyChannelUpstreamHandler;
import org.infinispan.server.core.netty.NettyServerBootstrap;
import org.infinispan.server.core.netty.memcached.NettyMemcachedDecoder;
import org.infinispan.server.core.InterceptorChain;

/**
 * TextServer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MemcachedTextServer {
   private final CacheManager manager;
   private final int port;
   private final ScheduledExecutorService scheduler;
   private ServerBootstrap bootstrap;
   
   public MemcachedTextServer(CacheManager manager, int port) {
      this.manager = manager;
      this.port = port;
      this.scheduler = Executors.newScheduledThreadPool(1);
   }

   public int getPort() {
      return port;
   }

   public void start() {
      // Configure Infinispan Cache instance
      Cache cache = manager.getCache();

      InterceptorChain chain = TextProtocolInterceptorChainFactory.getInstance(cache).buildInterceptorChain();
      NettyMemcachedDecoder decoder = new NettyMemcachedDecoder(cache, chain, scheduler);
      TextCommandHandler commandHandler = new TextCommandHandler(cache, chain);
      NettyChannelUpstreamHandler handler = new NettyChannelUpstreamHandler(commandHandler);
      NettyChannelPipelineFactory pipelineFactory = new NettyChannelPipelineFactory(decoder, handler);
      bootstrap = new NettyServerBootstrap(pipelineFactory, new InetSocketAddress(port));
      bootstrap.start();
   }

   public void stop() {
      bootstrap.stop();
      manager.stop();
      scheduler.shutdown();
   }
}
