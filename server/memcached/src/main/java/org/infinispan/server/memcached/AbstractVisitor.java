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

import org.jboss.netty.channel.Channel;

/**
 * CommandInterceptor.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractVisitor implements Visitor {

   @Override
   public Object visitAdd(Channel ch, AddCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitAppend(Channel ch, AppendCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitCas(Channel ch, CasCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitDecrement(Channel ch, DecrementCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitDelete(Channel ch, DeleteCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitGet(Channel ch, GetCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitGets(Channel ch, GetsCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitIncrement(Channel ch, IncrementCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitPrepend(Channel ch, PrependCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitReplace(Channel ch, ReplaceCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitSet(Channel ch, SetCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   @Override
   public Object visitStats(Channel ch, StatsCommand command) throws Exception {
      return handleDefault(ch, command);
   }

   protected Object handleDefault(Channel ch, Command command) throws Exception {
      return null;
   }
}
