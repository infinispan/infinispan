/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.infinispan.commands.write;

import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClearCommand implements WriteCommand {
   private static final Object[] params = new Object[0];
   public static final byte COMMAND_ID = 17;
   CacheNotifier notifier;

   public ClearCommand() {
   }

   public ClearCommand(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitClearCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      for (CacheEntry e : ctx.getLookedUpEntries().values()) {
         if (e instanceof MVCCEntry) {
            MVCCEntry me = (MVCCEntry) e;
            Object k = me.getKey(), v = me.getValue();
            notifier.notifyCacheEntryRemoved(k, v, true, ctx);
            me.setRemoved(true);
            me.setValid(false);
            notifier.notifyCacheEntryRemoved(k, null, false, ctx);
         }
      }
      return null;
   }

   public Object[] getParameters() {
      return params;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid command id");
   }

   @Override
   public String toString() {
      return "ClearCommand";
   }

   public boolean isSuccessful() {
      return true;
   }

   public boolean isConditional() {
      return false;
   }
}
