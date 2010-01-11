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
package org.infinispan.server.core.netty;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;

import org.infinispan.server.core.CommandHandler;
import org.infinispan.server.core.netty.memcached.NettyMemcachedDecoder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * NettyChannelFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyServerBootstrap implements org.infinispan.server.core.ServerBootstrap {
   private static final Log log = LogFactory.getLog(NettyServerBootstrap.class);
   final ChannelPipelineFactory pipeline;
   final SocketAddress address;
   final ChannelFactory factory;
   final ChannelGroup serverChannels = new DefaultChannelGroup("memcached-channels");
   final ChannelGroup acceptedChannels = new DefaultChannelGroup("memcached-accepted");
   
   public NettyServerBootstrap(CommandHandler commandHandler, NettyMemcachedDecoder decoder, SocketAddress address, 
            ExecutorService masterExecutor, ExecutorService workerExecutor, int workerThreads) {
      NettyChannelUpstreamHandler handler = new NettyChannelUpstreamHandler(commandHandler, acceptedChannels);
      this.pipeline = new NettyChannelPipelineFactory(decoder, handler);
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
      serverChannels.close().awaitUninterruptibly();
      ChannelGroupFuture future = acceptedChannels.close().awaitUninterruptibly();
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
}
