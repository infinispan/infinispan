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
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * NettyChannelFactory.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyServerBootstrap implements org.infinispan.server.core.ServerBootstrap {
   final ChannelPipelineFactory pipeline;
   final SocketAddress address;
   
   public NettyServerBootstrap(ChannelPipelineFactory pipeline, SocketAddress address) {
      this.pipeline = pipeline;
      this.address = address;
   }

   @Override
   public void start() {
      ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
      ServerBootstrap bootstrap = new ServerBootstrap(factory);
      bootstrap.setPipelineFactory(pipeline);
      bootstrap.bind(address);
   }

   @Override
   public void stop() {
      // TODO how to close shutdown the nettty server?
   }

   public static NettyServerBootstrap newNettyServerBootstrap(ChannelPipelineFactory pipeline, SocketAddress address) {
      return new NettyServerBootstrap(pipeline, address);
   }

}
