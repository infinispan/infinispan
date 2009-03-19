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


/**
 * @author Mircea.Markus@jboss.com
 * @since 1.0
 */
public class ReplaceCommand extends AbstractDataCommand implements DataWriteCommand {
   public static final byte METHOD_ID = 122;

   protected Object oldValue;
   protected Object newValue;
   protected long lifespanMillis = -1;
   boolean successful = true;

   public ReplaceCommand(Object key, Object oldValue, Object newValue, long lifespanMillis) {
      super(key);
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.lifespanMillis = lifespanMillis;
   }

   public ReplaceCommand(Object key, Object oldValue, Object newValue) {
      super(key);
      this.oldValue = oldValue;
      this.newValue = newValue;
   }

   public ReplaceCommand() {
   }

   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReplaceCommand(ctx, this);
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = ctx.lookupEntry(key);
      if (e != null) {
         if (ctx.isOriginLocal()) {
            if (e.isNullEntry()) return returnValue(null, false);

            if (oldValue == null || oldValue.equals(e.getValue())) {
               Object old = e.setValue(newValue);
               e.setLifespan(lifespanMillis);
               return returnValue(old, true);
            }
            return returnValue(null, false);
         } else {
            // for remotely originating calls, this doesn't check the status of what is under the key at the moment
            Object old = e.setValue(newValue);
            e.setLifespan(lifespanMillis);
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
      return METHOD_ID;
   }

   public Object[] getParameters() {
      if (lifespanMillis < 0)
         return new Object[]{key, oldValue, newValue, false};
      else
         return new Object[]{key, oldValue, newValue, true, lifespanMillis};
   }

   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != METHOD_ID) throw new IllegalArgumentException("Invalid method name");
      key = parameters[0];
      oldValue = parameters[1];
      newValue = parameters[2];
      boolean setLifespan = (Boolean) parameters[3];
      if (setLifespan) lifespanMillis = (Long) parameters[4];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ReplaceCommand that = (ReplaceCommand) o;

      if (lifespanMillis != that.lifespanMillis) return false;
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
      return result;
   }

   public boolean isSuccessful() {
      return successful;
   }

   public long getLifespanMillis() {
      return lifespanMillis;
   }

   @Override
   public String toString() {
      return "ReplaceCommand{" +
            "oldValue=" + oldValue +
            ", newValue=" + newValue +
            ", lifespanMillis=" + lifespanMillis +
            ", successful=" + successful +
            '}';
   }
}
