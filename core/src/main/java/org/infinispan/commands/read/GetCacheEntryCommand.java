/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
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

package org.infinispan.commands.read;

import org.infinispan.commands.Visitor;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;

import java.util.Set;

/**
 * An internal cache get command that returns
 * {@link org.infinispan.container.entries.CacheEntry}
 * instead of just the value.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class GetCacheEntryCommand extends GetKeyValueCommand {

   public static final byte COMMAND_ID = 33;

   @SuppressWarnings("unused")
   private GetCacheEntryCommand() {
      // For command id uniqueness test
   }

   public GetCacheEntryCommand(Object key, Set<Flag> flags) {
      super(key, flags);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetCacheEntryCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Object ret = super.perform(ctx);
      if (ret != null)
         return ctx.lookupEntry(key);

      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

}
