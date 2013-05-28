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

import org.infinispan.metadata.Metadata;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.Equivalence;

import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplaceCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   public static final byte COMMAND_ID = 11;

   Object oldValue;
   Object newValue;
   Metadata metadata;
   private CacheNotifier notifier;
   boolean successful = true;

   boolean ignorePreviousValue;
   private Equivalence valueEquivalence;

   public ReplaceCommand() {
   }

   public ReplaceCommand(Object key, Object oldValue, Object newValue,
         CacheNotifier notifier, Metadata metadata, Set<Flag> flags, Equivalence valueEquivalence) {
      super(key, flags);
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.notifier = notifier;
      this.metadata = metadata;
      this.valueEquivalence = valueEquivalence;
   }
   
   public void init(CacheNotifier notifier, Configuration cfg) {
      this.notifier = notifier;
      this.valueEquivalence = cfg.dataContainer().valueEquivalence();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitReplaceCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e != null) {
         if (ctx.isOriginLocal()) {
            //ISPN-514
            if (e.isNull() || e.getValue() == null) {
               // Revert assumption that new value is to be committed
               e.setChanged(false);
               return returnValue(null, false, ctx);
            }
         }

         if (oldValue == null || isValueEquals(oldValue, e.getValue()) || ignorePreviousValue) {
            e.setChanged(true);
            Object old = e.setValue(newValue);
            return returnValue(old, true, ctx);
         }
         // Revert assumption that new value is to be committed
         e.setChanged(false);
      }

      return returnValue(null, false, ctx);
   }

   @SuppressWarnings("unchecked")
   private boolean isValueEquals(Object oldValue, Object newValue) {
      if (valueEquivalence != null)
         return valueEquivalence.equals(oldValue, newValue);

      return oldValue.equals(newValue);
   }

   private Object returnValue(Object beingReplaced, boolean successful, 
         InvocationContext ctx) {
      this.successful = successful;
      
      Object previousValue = oldValue == null ? beingReplaced : oldValue;

      if (successful) {
         notifier.notifyCacheEntryModified(
               key, previousValue, previousValue == null, true, ctx, this);
      }

      if (oldValue == null) {
         return beingReplaced;
      } else {
         return successful;
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, oldValue, newValue, metadata, ignorePreviousValue,
                          Flag.copyWithoutRemotableFlags(flags),previousRead};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalArgumentException("Invalid method name");
      key = parameters[0];
      oldValue = parameters[1];
      newValue = parameters[2];
      metadata = (Metadata) parameters[3];
      ignorePreviousValue = (Boolean) parameters[4];
      flags = (Set<Flag>) parameters[5];
      previousRead = (Boolean) parameters[6];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      ReplaceCommand that = (ReplaceCommand) o;

      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
      if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
      result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return !ignorePreviousValue;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   public Object getOldValue() {
      return oldValue;
   }

   public void setOldValue(Object oldValue) {
      this.oldValue = oldValue;
   }

   public Object getNewValue() {
      return newValue;
   }

   public void setNewValue(Object newValue) {
      this.newValue = newValue;
   }

   public boolean isIgnorePreviousValue() {
      return ignorePreviousValue;
   }

   public void setIgnorePreviousValue(boolean ignorePreviousValue) {
      this.ignorePreviousValue = ignorePreviousValue;
   }

   @Override
   public final boolean isReturnValueExpected() {
     //SKIP_RETURN_VALUE ignored for conditional replace
     return super.isReturnValueExpected() || isConditional();
   }

   @Override
   public String toString() {
      return "ReplaceCommand{" +
            "key=" + toStr(key) +
            ", oldValue=" + toStr(oldValue) +
            ", newValue=" + toStr(newValue) +
            ", metadata=" + metadata +
            ", flags=" + flags +
            ", successful=" + successful +
            ", ignorePreviousValue=" + ignorePreviousValue +
            '}';
   }
}
