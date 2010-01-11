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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.ServerBootstrap;
import org.infinispan.server.core.netty.NettyServerBootstrap;
import org.infinispan.server.core.netty.memcached.NettyMemcachedDecoder;
import org.infinispan.server.core.InterceptorChain;
import org.infinispan.server.memcached.commands.TextCommandHandler;
import org.infinispan.server.memcached.interceptors.TextProtocolInterceptorChainFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * TextServer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class TextServer {
   private static final Log log = LogFactory.getLog(TextServer.class);
   private final Cache cache;
   private final String host;
   private final int port;
   private final int masterThreads;
   private final int workerThreads;
   private final ScheduledExecutorService scheduler;
   private ServerBootstrap bootstrap;
   private ExecutorService masterExecutor;
   private ExecutorService workerExecutor;
   private final AtomicInteger masterThreadNumber = new AtomicInteger(1);
   private final AtomicInteger workerThreadNumber = new AtomicInteger(1);

   public TextServer(String host, int port, String configFile, int masterThreads, int workerThreads) throws IOException {
      this(host, port, configFile == null 
               ? new DefaultCacheManager().getCache() 
               : new DefaultCacheManager(configFile).getCache(), 
               masterThreads, masterThreads);
      if (configFile == null) {
         log.debug("Using cache manager using configuration defaults");
      } else {
         log.debug("Using cache manager configured from {0}", configFile);
      }
   }

   public TextServer(String host, int port, Cache cache, int masterThreads, int workerThreads) throws IOException {
      this.host = host;
      this.port = port;
      this.masterThreads = masterThreads;
      this.workerThreads = workerThreads;
      this.cache = cache;
      this.scheduler = Executors.newScheduledThreadPool(1);
   }

   public int getPort() {
      return port;
   }

   public void start() throws Exception {
      InterceptorChain chain = TextProtocolInterceptorChainFactory.getInstance(cache).buildInterceptorChain();
      NettyMemcachedDecoder decoder = new NettyMemcachedDecoder(cache, chain, scheduler);
      TextCommandHandler commandHandler = new TextCommandHandler(cache, chain);

      bootstrap = new NettyServerBootstrap(commandHandler, decoder, new InetSocketAddress(host, port), 
               masterThreads, workerThreads, cache.getName());
      bootstrap.start();
      log.info("Started Memcached text server bound to {0}:{1}", host, port);
   }

   public void stop() {
      masterExecutor.shutdown();
      workerExecutor.shutdown();
      bootstrap.stop();
      cache.stop();
      scheduler.shutdown();
   }

}
