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

import org.infinispan.server.core.transport.Channel;
import org.infinispan.server.core.transport.ChannelBuffer;
import org.infinispan.server.core.transport.ChannelFuture;

/**
 * NettyChannel.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyChannel implements Channel {
   final org.jboss.netty.channel.Channel ch;

   public NettyChannel(org.jboss.netty.channel.Channel ch) {
      this.ch = ch;
   }
   
   @Override
   public ChannelFuture disconnect() {
      return new NettyChannelFuture(ch.disconnect());
   }

   @Override
   public ChannelFuture write(Object message) {
      if (message instanceof ChannelBuffer) {
         // If we get an Infinispan channel buffer abstraction, convert it to something netty can understand
         message = ((ChannelBuffer) message).getUnderlyingChannelBuffer();
      }
      return new NettyChannelFuture(ch.write(message));
   }

   @Override
   public int compareTo(Channel o) {
      return ch.compareTo(((NettyChannel) o).ch);
   }

}
