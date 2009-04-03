/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.horizon.atomic.Delta;
import org.horizon.atomic.DeltaAware;
import org.horizon.commands.Visitor;
import org.horizon.commands.read.AbstractDataCommand;
import org.horizon.container.entries.MVCCEntry;
import org.horizon.context.InvocationContext;
import org.horizon.notifications.cachelistener.CacheNotifier;

/**
 * Implements functionality defined by {@link org.horizon.Cache#put(Object, Object)}
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutKeyValueCommand extends AbstractDataCommand implements DataWriteCommand {
   public static final byte METHOD_ID = 3;

   Object value;
   boolean putIfAbsent;
   CacheNotifier notifier;
   boolean successful = true;
   long lifespanMillis = -1;
   long maxIdleTimeMillis = -1;

   public PutKeyValueCommand() {
   }

   public PutKeyValueCommand(Object key, Object value, boolean putIfAbsent, CacheNotifier notifier, long lifespanMillis, long maxIdleTimeMillis) {
      super(key);
      this.value = value;
      this.putIfAbsent = putIfAbsent;
      this.notifier = notifier;
      this.lifespanMillis = lifespanMillis;
      this.maxIdleTimeMillis = maxIdleTimeMillis;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutKeyValueCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      Object o;
      MVCCEntry e = lookupMvccEntry(ctx, key);
      if (e.getValue() != null && putIfAbsent) {
         successful = false;
         return e.getValue();
      } else {
         notifier.notifyCacheEntryModified(key, e.getValue(), true, ctx);

         if (value instanceof Delta) {
            // magic
            Delta dv = (Delta) value;
            Object existing = e.getValue();
            DeltaAware toMergeWith = null;
            if (existing instanceof DeltaAware) toMergeWith = (DeltaAware) existing;
            e.setValue(dv.merge(toMergeWith));
            o = existing;
            e.setLifespan(lifespanMillis);
            e.setMaxIdle(maxIdleTimeMillis);
         } else {
            o = e.setValue(value);
            e.setLifespan(lifespanMillis);
            e.setMaxIdle(maxIdleTimeMillis);
         }
         notifier.notifyCacheEntryModified(key, e.getValue(), false, ctx);
      }
      return o;
   }

   public byte getCommandId() {
      return METHOD_ID;
   }

   public Object[] getParameters() {
      return new Object[]{key, value, lifespanMillis, maxIdleTimeMillis};
   }

   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != METHOD_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      value = parameters[1];
      lifespanMillis = (Long) parameters[2];
      maxIdleTimeMillis = (Long) parameters[3];
   }

   public boolean isPutIfAbsent() {
      return putIfAbsent;
   }

   public void setPutIfAbsent(boolean putIfAbsent) {
      this.putIfAbsent = putIfAbsent;
   }

   public long getLifespanMillis() {
      return lifespanMillis;
   }

   public long getMaxIdleTimeMillis() {
      return maxIdleTimeMillis;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      PutKeyValueCommand that = (PutKeyValueCommand) o;

      if (lifespanMillis != that.lifespanMillis) return false;
      if (maxIdleTimeMillis != that.maxIdleTimeMillis) return false;
      if (putIfAbsent != that.putIfAbsent) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (putIfAbsent ? 1 : 0);
      result = 31 * result + (int) (lifespanMillis ^ (lifespanMillis >>> 32));
      result = 31 * result + (int) (maxIdleTimeMillis ^ (maxIdleTimeMillis >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "PutKeyValueCommand{" +
            "key=" + key +
            ", value=" + value +
            ", putIfAbsent=" + putIfAbsent +
            '}';
   }

   public boolean isSuccessful() {
      return successful;
   }
}