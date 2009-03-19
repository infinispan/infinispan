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
package org.horizon.commands.write;

import org.horizon.commands.Visitor;
import org.horizon.commands.read.AbstractDataCommand;
import org.horizon.container.MVCCEntry;
import org.horizon.context.InvocationContext;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.notifications.cachelistener.CacheNotifier;


/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoveCommand extends AbstractDataCommand implements DataWriteCommand {
   private static final Log log = LogFactory.getLog(RemoveCommand.class);
   private static final boolean trace = log.isTraceEnabled();
   public static final byte METHOD_ID = 6;
   protected CacheNotifier notifier;
   boolean successful = true;

   protected Object value;

   public RemoveCommand(Object key, Object value, CacheNotifier notifier) {
      super(key);
      this.value = value;
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public RemoveCommand() {
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = ctx.lookupEntry(key);
      if (e == null || e.isNullEntry()) {
         log.trace("Nothing to remove since the entry is null or we have a null entry");
         if (value == null) {
            return null;
         } else {
            successful = false;
            return false;
         }
      }
      if (value != null && e.getValue() != null && !e.getValue().equals(value)) {
         successful = false;
         return false;
      }

      notify(ctx, e.getValue(), true);
      e.setDeleted(true);
      e.setValid(false);
      notify(ctx, null, false);
      return value == null ? e.getValue() : true;
   }

   protected void notify(InvocationContext ctx, Object value, boolean isPre) {
      notifier.notifyCacheEntryRemoved(key, value, isPre, ctx);
   }

   public byte getCommandId() {
      return METHOD_ID;
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoveCommand)) return false;
      if (!super.equals(o)) return false;

      RemoveCommand that = (RemoveCommand) o;

      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
   }

   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }


   public String toString() {
      return getClass().getSimpleName() + "{" +
            "key=" + key +
            ", value=" + value +
            '}';
   }

   public boolean isSuccessful() {
      return successful;
   }
}
