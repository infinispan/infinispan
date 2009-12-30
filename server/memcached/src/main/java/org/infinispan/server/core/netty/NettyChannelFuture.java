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

import java.util.concurrent.TimeUnit;

import org.infinispan.server.core.Channel;
import org.infinispan.server.core.ChannelFuture;

/**
 * NettyChannelFuture.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyChannelFuture implements ChannelFuture {
   final org.jboss.netty.channel.ChannelFuture future;
   final Channel ch;

   public NettyChannelFuture(org.jboss.netty.channel.ChannelFuture future, Channel ch) {
      this.future = future;
      this.ch = ch;
   }

   @Override
   public ChannelFuture await() throws InterruptedException {
      future.await();
      return this;
   }

   @Override
   public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
      return future.await(timeout, unit);
   }

   @Override
   public boolean await(long timeoutMillis) throws InterruptedException {
      return future.await(timeoutMillis);
   }

   @Override
   public ChannelFuture awaitUninterruptibly() {
      future.awaitUninterruptibly();
      return this;
   }

   @Override
   public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
      return future.awaitUninterruptibly(timeout, unit);
   }

   @Override
   public boolean awaitUninterruptibly(long timeoutMillis) {
      return future.awaitUninterruptibly(timeoutMillis);
   }

   @Override
   public Channel getChannel() {
      return ch;
   }

   @Override
   public boolean isCancelled() {
      return future.isCancelled();
   }

   @Override
   public boolean isDone() {
      return future.isDone();
   }

   @Override
   public boolean setFailure(Throwable cause) {
      return future.setFailure(cause);
   }

   @Override
   public boolean setSuccess() {
      return future.setSuccess();
   }

}
