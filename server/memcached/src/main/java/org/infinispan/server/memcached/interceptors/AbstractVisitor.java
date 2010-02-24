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
package org.infinispan.server.memcached.interceptors;

import org.infinispan.server.core.transport.ChannelHandlerContext;
import org.infinispan.server.memcached.commands.AddCommand;
import org.infinispan.server.memcached.commands.AppendCommand;
import org.infinispan.server.memcached.commands.CasCommand;
import org.infinispan.server.memcached.commands.DecrementCommand;
import org.infinispan.server.memcached.commands.DeleteCommand;
import org.infinispan.server.memcached.commands.FlushAllCommand;
import org.infinispan.server.memcached.commands.GetCommand;
import org.infinispan.server.memcached.commands.GetsCommand;
import org.infinispan.server.memcached.commands.IncrementCommand;
import org.infinispan.server.memcached.commands.PrependCommand;
import org.infinispan.server.memcached.commands.QuitCommand;
import org.infinispan.server.memcached.commands.ReplaceCommand;
import org.infinispan.server.memcached.commands.SetCommand;
import org.infinispan.server.memcached.commands.StatsCommand;
import org.infinispan.server.memcached.commands.TextCommand;
import org.infinispan.server.memcached.commands.VersionCommand;

/**
 * CommandInterceptor.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class AbstractVisitor implements TextProtocolVisitor {

   @Override
   public Object visitAdd(ChannelHandlerContext ctx, AddCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitAppend(ChannelHandlerContext ctx, AppendCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitCas(ChannelHandlerContext ctx, CasCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitDecrement(ChannelHandlerContext ctx, DecrementCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitDelete(ChannelHandlerContext ctx, DeleteCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitGet(ChannelHandlerContext ctx, GetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitGets(ChannelHandlerContext ctx, GetsCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitIncrement(ChannelHandlerContext ctx, IncrementCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitPrepend(ChannelHandlerContext ctx, PrependCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitReplace(ChannelHandlerContext ctx, ReplaceCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitSet(ChannelHandlerContext ctx, SetCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitStats(ChannelHandlerContext ctx, StatsCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitFlushAll(ChannelHandlerContext ctx, FlushAllCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitVersion(ChannelHandlerContext ctx, VersionCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   @Override
   public Object visitQuit(ChannelHandlerContext ctx, QuitCommand command) throws Throwable {
      return handleDefault(ctx, command);
   }

   protected Object handleDefault(ChannelHandlerContext ctx, TextCommand command) throws Throwable {
      return null;
   }
}
