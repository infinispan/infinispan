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

import org.infinispan.server.core.ChannelHandlerContext;

/**
 * CommandInterceptor.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface TextProtocolVisitor {
   Object visitSet(ChannelHandlerContext ctx, SetCommand command) throws Throwable;
   Object visitAdd(ChannelHandlerContext ctx, AddCommand command) throws Throwable;
   Object visitReplace(ChannelHandlerContext ctx, ReplaceCommand command) throws Throwable;
   Object visitAppend(ChannelHandlerContext ctx, AppendCommand command) throws Throwable;
   Object visitPrepend(ChannelHandlerContext ctx, PrependCommand command) throws Throwable;
   Object visitCas(ChannelHandlerContext ctx, CasCommand command) throws Throwable;
   Object visitGet(ChannelHandlerContext ctx, GetCommand command) throws Throwable;
   Object visitGets(ChannelHandlerContext ctx, GetsCommand command) throws Throwable;
   Object visitDelete(ChannelHandlerContext ctx, DeleteCommand command) throws Throwable;
   Object visitIncrement(ChannelHandlerContext ctx, IncrementCommand command) throws Throwable;
   Object visitDecrement(ChannelHandlerContext ctx, DecrementCommand command) throws Throwable;
   Object visitStats(ChannelHandlerContext ctx, StatsCommand command) throws Throwable;
   Object visitFlushAll(ChannelHandlerContext ctx, FlushAllCommand command) throws Throwable;
   Object visitVersion(ChannelHandlerContext ctx, VersionCommand command) throws Throwable;
   Object visitQuit(ChannelHandlerContext ctx, QuitCommand command) throws Throwable;
}
