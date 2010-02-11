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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.Server;
import org.infinispan.server.core.transport.netty.NettyServer;
import org.infinispan.server.memcached.transport.netty.NettyMemcachedDecoder;
import org.infinispan.server.core.InterceptorChain;
import org.infinispan.server.memcached.commands.TextCommandHandler;
import org.infinispan.server.memcached.commands.Value;
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
   private final Cache<String, Value> cache;
   private final String host;
   private final int port;
   private final int masterThreads;
   private final int workerThreads;
   private final ScheduledExecutorService scheduler;
   private Server server;

   public TextServer(String host, int port, String configFile, int masterThreads, int workerThreads) throws IOException {
      this(host, port, configFile == null 
               ? new DefaultCacheManager().<String, Value>getCache()
               : new DefaultCacheManager(configFile).<String, Value>getCache(),
               masterThreads, workerThreads);
      if (configFile == null) {
         log.debug("Using cache manager using configuration defaults");
      } else {
         log.debug("Using cache manager configured from {0}", configFile);
      }
   }

   public TextServer(String host, int port, Cache<String, Value> cache, int masterThreads, int workerThreads) throws IOException {
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

      server = new NettyServer(commandHandler, decoder, new InetSocketAddress(host, port),
               masterThreads, workerThreads, cache.getName());
      server.start();
      log.info("Started Memcached text server bound to {0}:{1}", host, port);
   }

   public void stop() {
      if (server != null) {
         server.stop();
      }
      cache.stop();
      scheduler.shutdown();
   }

}
