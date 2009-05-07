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
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;


/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ReplaceCommand extends AbstractDataCommand implements DataWriteCommand {
   public static final byte COMMAND_ID = 11;

   Object oldValue;
   Object newValue;
   long lifespanMillis = -1;
   long maxIdleTimeMillis = -1;
   boolean successful = true;

   public ReplaceCommand() {
   }

   public ReplaceCommand(Object key, Object oldValue, Object newValue, long lifespanMillis, long maxIdleTimeMillis) {
      super(key);
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.lifespanMillis = lifespanMillis;
      this.maxIdleTimeMillis = maxIdleTimeMillis;
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReplaceCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = lookupMvccEntry(ctx, key);
      if (e != null) {
         if (ctx.isOriginLocal()) {
            if (e.isNull()) return returnValue(null, false);

            if (oldValue == null || oldValue.equals(e.getValue())) {
               Object old = e.setValue(newValue);
               e.setLifespan(lifespanMillis);
               e.setMaxIdle(maxIdleTimeMillis);
               return returnValue(old, true);
            }
            return returnValue(null, false);
         } else {
            // for remotely originating calls, this doesn't check the status of what is under the key at the moment
            Object old = e.setValue(newValue);
            e.setLifespan(lifespanMillis);
            e.setMaxIdle(maxIdleTimeMillis);
            return returnValue(old, true);
         }
      }

      return returnValue(null, false);
   }

   private Object returnValue(Object beingReplaced, boolean successful) {
      this.successful = successful;
      if (oldValue == null) {
         return beingReplaced;
      } else {
         return successful;
      }
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{key, oldValue, newValue, lifespanMillis, maxIdleTimeMillis};
   }

   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalArgumentException("Invalid method name");
      key = parameters[0];
      oldValue = parameters[1];
      newValue = parameters[2];
      lifespanMillis = (Long) parameters[3];
      maxIdleTimeMillis = (Long) parameters[4];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ReplaceCommand that = (ReplaceCommand) o;

      if (lifespanMillis != that.lifespanMillis) return false;
      if (maxIdleTimeMillis != that.maxIdleTimeMillis) return false;
      if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
      if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
      result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
      result = 31 * result + (int) (lifespanMillis ^ (lifespanMillis >>> 32));
      result = 31 * result + (int) (maxIdleTimeMillis ^ (maxIdleTimeMillis >>> 32));
      return result;
   }

   public boolean isSuccessful() {
      return successful;
   }

   public boolean isConditional() {
      return true;
   }

   public long getLifespanMillis() {
      return lifespanMillis;
   }

   public long getMaxIdleTimeMillis() {
      return maxIdleTimeMillis;
   }

   @Override
   public String toString() {
      return "ReplaceCommand{" +
            "oldValue=" + oldValue +
            ", newValue=" + newValue +
            ", successful=" + successful +
            '}';
   }
}
