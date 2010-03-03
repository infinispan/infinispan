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
package org.infinispan.server.core.transport.netty;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.server.core.CommandHandler;
import org.infinispan.server.core.Server;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * NettyChannelFactory.
 *
 * // TODO: Make this class more generic and remove any memcached references
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyServer implements Server {
   private static final Log log = LogFactory.getLog(NettyServer.class);
   final ChannelPipelineFactory pipeline;
   final SocketAddress address;
   final ChannelFactory factory;
   final ChannelGroup serverChannels = new DefaultChannelGroup("memcached-channels");
   final ChannelGroup acceptedChannels = new DefaultChannelGroup("memcached-accepted");
   final ExecutorService masterExecutor;
   final ExecutorService workerExecutor;
   
   public NettyServer(CommandHandler commandHandler, ChannelUpstreamHandler decoder, ChannelDownstreamHandler encoder, 
            SocketAddress address, int masterThreads, int workerThreads, String cacheName) {
      ThreadFactory tf = new MemcachedThreadFactory(cacheName, ExecutorType.MASTER);
      if (masterThreads == 0) {
         log.debug("Configured unlimited threads for master thread pool");
         masterExecutor = Executors.newCachedThreadPool(tf);
      } else {
         log.debug("Configured {0} threads for master thread pool", masterThreads);
         masterExecutor = Executors.newFixedThreadPool(masterThreads, tf);
      }

      tf = new MemcachedThreadFactory(cacheName, ExecutorType.WORKER);
      if (workerThreads == 0) {
         log.debug("Configured unlimited threads for worker thread pool");
         workerExecutor = Executors.newCachedThreadPool(tf);
      } else {
         log.debug("Configured {0} threads for worker thread pool", workerThreads);
         workerExecutor = Executors.newFixedThreadPool(workerThreads, tf);
      }

      NettyChannelUpstreamHandler handler = new NettyChannelUpstreamHandler(commandHandler, acceptedChannels);
      this.pipeline = new NettyChannelPipelineFactory(decoder, encoder, handler);
      this.address = address;
      if (workerThreads == 0) {
         factory = new NioServerSocketChannelFactory(masterExecutor, workerExecutor);
      } else {
         factory = new NioServerSocketChannelFactory(masterExecutor, workerExecutor, workerThreads);
      }
   }

   @Override
   public void start() {
      ServerBootstrap bootstrap = new ServerBootstrap(factory);
      bootstrap.setPipelineFactory(pipeline);
      Channel ch = bootstrap.bind(address);
      serverChannels.add(ch);
   }

   @Override
   public void stop() {
      // We *pause* the acceptor so no new connections are made
      ChannelGroupFuture future = serverChannels.unbind().awaitUninterruptibly();
      if (!future.isCompleteSuccess()) {
         log.warn("Server channel group did not completely unbind");
         for (Channel ch : future.getGroup()) {
            if (ch.isBound()) {
               log.warn("{0} is still bound to {1}", ch, ch.getRemoteAddress());
            }
         }
      }

      // TODO remove workaround when integrating Netty 3.2.x - https://jira.jboss.org/jira/browse/NETTY-256
      masterExecutor.shutdown();
      try {
         masterExecutor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }

      workerExecutor.shutdown();
      serverChannels.close().awaitUninterruptibly();
      future = acceptedChannels.close().awaitUninterruptibly();
      if (!future.isCompleteSuccess()) {
         log.warn("Channel group did not completely close");
         for (Channel ch : future.getGroup()) {
            if (ch.isBound()) {
               log.warn(ch + " is still connected to " + ch.getRemoteAddress());
            }
         }
      }
      log.debug("Channel group completely closed, release external resources");
      factory.releaseExternalResources();
   }

   private static class MemcachedThreadFactory implements ThreadFactory {
      final String cacheName;
      final ExecutorType type;

      MemcachedThreadFactory(String cacheName, ExecutorType type) {
         this.cacheName = cacheName;
         this.type = type;
      }

      @Override
      public Thread newThread(Runnable r) {
         Thread t = new Thread(r, System.getProperty("program.name") + "-" + cacheName + '-' + type.toString().toLowerCase() + '-' + type.getAndIncrement());
         t.setDaemon(true);
         return t;
      }
   }

   private static enum ExecutorType {
      MASTER(1), WORKER(1);

      final AtomicInteger threadCounter;

      ExecutorType(int startIndex) {
         this.threadCounter = new AtomicInteger(startIndex);
      }

      int getAndIncrement() {
         return threadCounter.getAndIncrement();
      }
   }
}
