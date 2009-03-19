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
import org.horizon.container.MVCCEntry;
import org.horizon.context.InvocationContext;
import org.horizon.notifications.cachelistener.CacheNotifier;

import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Mircea.Markus@jboss.com
 * @since 1.0
 */
public class PutMapCommand implements WriteCommand {
   public static final byte METHOD_ID = 121;

   private Map<Object, Object> map;
   private CacheNotifier notifier;
   private long lifespanMillis = -1;

   public PutMapCommand(Map map, CacheNotifier notifier, long lifespanMillis) {
      this.map = map;
      this.notifier = notifier;
      this.lifespanMillis = lifespanMillis;
   }

   public PutMapCommand(Map map, CacheNotifier notifier) {
      this.map = map;
      this.notifier = notifier;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public PutMapCommand() {
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutMapCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      for (Entry<Object, Object> e : map.entrySet()) {
         Object key = e.getKey();
         MVCCEntry me = ctx.lookupEntry(key);
         notifier.notifyCacheEntryModified(key, me.getValue(), true, ctx);
         me.setValue(e.getValue());
         me.setLifespan(lifespanMillis);
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

   public byte getCommandId() {
      return METHOD_ID;
   }

   public Object[] getParameters() {
      if (lifespanMillis < 0)
         return new Object[]{map, false};
      else
         return new Object[]{map, true, lifespanMillis};
   }

   public void setParameters(int commandId, Object[] parameters) {
      map = (Map) parameters[0];
      boolean setLifespan = (Boolean) parameters[1];
      if (setLifespan) lifespanMillis = (Long) parameters[2];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PutMapCommand that = (PutMapCommand) o;

      if (lifespanMillis != that.lifespanMillis) return false;
      if (map != null ? !map.equals(that.map) : that.map != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = map != null ? map.hashCode() : 0;
      result = 31 * result + (int) (lifespanMillis ^ (lifespanMillis >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "PutMapCommand{" +
            "map=" + map +
            ", lifespanMillis=" + lifespanMillis +
            '}';
   }

   public boolean isSuccessful() {
      return true;
   }

   public long getLifespanMillis() {
      return lifespanMillis;
   }
}
