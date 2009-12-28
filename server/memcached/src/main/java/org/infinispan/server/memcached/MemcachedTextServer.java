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

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * TextServer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MemcachedTextServer {
   private final CacheManager manager;
   private final int port;
   
   public MemcachedTextServer(CacheManager manager, int port) {
      this.manager = manager;
      this.port = port;
   }

   public int getPort() {
      return port;
   }

   public void start() {
      // Configure Infinispan Cache instance
      Cache cache = manager.getCache();

      // Configure the server.
      ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
      ServerBootstrap bootstrap = new ServerBootstrap(factory);
      InterceptorChain chain = InterceptorChainFactory.getInstance(cache).buildInterceptorChain();
      bootstrap.setPipelineFactory(new TextProtocolPipelineFactory(cache, chain));
      bootstrap.bind(new InetSocketAddress(port));
   }

   public void stop() {
      manager.stop();
   }
}
