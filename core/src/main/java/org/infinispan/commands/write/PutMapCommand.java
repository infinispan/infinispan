/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.commands.write;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutMapCommand extends AbstractFlagAffectedCommand implements WriteCommand {
   public static final byte COMMAND_ID = 9;

   Map<Object, Object> map;
   CacheNotifier notifier;
   long lifespanMillis = -1;
   long maxIdleTimeMillis = -1;

   public PutMapCommand() {
   }

   public PutMapCommand(Map map, CacheNotifier notifier, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags) {
      this.map = map;
      this.notifier = notifier;
      this.lifespanMillis = lifespanMillis;
      this.maxIdleTimeMillis = maxIdleTimeMillis;
      this.flags = flags;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
   }

   private MVCCEntry lookupMvccEntry(InvocationContext ctx, Object key) {
      return (MVCCEntry) ctx.lookupEntry(key);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      for (Entry<Object, Object> e : map.entrySet()) {
         Object key = e.getKey();
         MVCCEntry me = lookupMvccEntry(ctx, key);
         notifier.notifyCacheEntryModified(key, me.getValue(), true, ctx);
         me.setValue(e.getValue());
         me.setLifespan(lifespanMillis);
         me.setMaxIdle(maxIdleTimeMillis);
         notifier.notifyCacheEntryModified(key, me.getValue(), false, ctx);
      }
      return null;
   }

   public Map<Object, Object> getMap() {
      return map;
   }

   public void setMap(Map<Object, Object> map) {
      this.map = map;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{map, lifespanMillis, maxIdleTimeMillis, flags};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      map = (Map) parameters[0];
      lifespanMillis = (Long) parameters[1];
      maxIdleTimeMillis = (Long) parameters[2];
      if (parameters.length>3) {
         this.flags = (Set<Flag>) parameters[3];
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PutMapCommand that = (PutMapCommand) o;

      if (lifespanMillis != that.lifespanMillis) return false;
      if (maxIdleTimeMillis != that.maxIdleTimeMillis) return false;
      if (map != null ? !map.equals(that.map) : that.map != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = map != null ? map.hashCode() : 0;
      result = 31 * result + (int) (lifespanMillis ^ (lifespanMillis >>> 32));
      result = 31 * result + (int) (maxIdleTimeMillis ^ (maxIdleTimeMillis >>> 32));
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutMapCommand{map={");
      if (!map.isEmpty()) {
         Iterator<Entry<Object, Object>> it = map.entrySet().iterator();
         int i = 0;
         for (;;) {
            Entry<Object, Object> e = it.next();
            sb.append(e.getKey()).append('=').append(e.getValue());
            if (!it.hasNext()) {
               break;
            }
            if (i > 100) {
               sb.append(" ...");
               break;
            }
            sb.append(", ");
            i++;
         }
      }
      sb.append("}, flags=").append(flags)
         .append(", lifespanMillis=").append(lifespanMillis)
         .append(", maxIdleTimeMillis=").append(maxIdleTimeMillis)
         .append("}");
      return sb.toString();
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return map.keySet();
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   public long getLifespanMillis() {
      return lifespanMillis;
   }

   public long getMaxIdleTimeMillis() {
      return maxIdleTimeMillis;
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

}
