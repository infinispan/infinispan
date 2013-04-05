/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import java.io.Serializable;
import java.util.*;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.Visitor;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;


/**
 * @author Vladimir Blagojevic
 * @since 5.1
 */
public class ApplyDeltaCommand extends AbstractDataWriteCommand {
   
   public static final int COMMAND_ID = 25;
   private Object deltaAwareValueKey;
   private Collection<Object> keys;
   private Delta delta;
   
   public ApplyDeltaCommand() {
      super();
   }
   
   public ApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection<Object> keys) {
      super(deltaAwareValueKey, EnumSet.of(Flag.DELTA_WRITE));
      if (keys == null || keys.isEmpty())
         throw new IllegalArgumentException("At least one key to be locked needs to be specified");
      else
         this.keys = keys;
      
      this.delta = delta;
      this.deltaAwareValueKey = deltaAwareValueKey;      
   }
   
   public Object getDeltaAwareKey(){
      return deltaAwareValueKey;
   }
   
   public Delta getDelta(){
      return delta;
   }
   
   @Override
   public Object getKey() {
      return getDeltaAwareKey();
   }

   /**
    * Performs an application of delta on a specified entry
    *
    * @param ctx invocation context
    * @return null
    */
   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      //nothing to do here
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "ApplyDeltaCommand[key=" + deltaAwareValueKey + ", delta=" + delta + ",keys=" + keys+ ']';
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{deltaAwareValueKey, delta, keys, Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      // TODO: Check duplicated in all commands? A better solution is needed.
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Unusupported command id:" + commandId);
      int i = 0;
      deltaAwareValueKey = args[i++];
      delta = (Delta)args[i++];
      keys = (List<Object>) args[i++];
      flags = (Set<Flag>) args[i];
   }
   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitApplyDeltaCommand(ctx, this);
   }

   public Object[] getKeys() {
      return keys.toArray();
   }
   
   public Object[] getCompositeKeys(){
      List<DeltaCompositeKey> composite = new ArrayList<DeltaCompositeKey>(keys.size());
      for (Object k : keys) {
         composite.add(new DeltaCompositeKey(deltaAwareValueKey, k));         
      }      
      return composite.toArray();      
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      switch (status) {
         case FAILED:
         case INITIALIZING:
         case STOPPING:
         case TERMINATED:
            return true;
         default:
            return false;
         }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof ApplyDeltaCommand)) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      ApplyDeltaCommand that = (ApplyDeltaCommand) o;

      if (!keys.equals(that.keys)) {
         return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (keys != null ? keys.hashCode() : 0);
      return result;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }
   
   /**
    * DeltaCompositeKey is the key guarding access to a specific entry in DeltaAware
    * 
    */
   private static final class DeltaCompositeKey implements Serializable {
      /** The serialVersionUID */
      private static final long serialVersionUID = -8598408570487324159L;
      
      private final Object deltaAwareValueKey;
      private final Object entryKey;
      
      public DeltaCompositeKey(Object deltaAwareValueKey, Object entryKey) {         
         this.deltaAwareValueKey = deltaAwareValueKey;
         this.entryKey = entryKey;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result
                  + ((deltaAwareValueKey == null) ? 0 : deltaAwareValueKey.hashCode());
         result = prime * result + ((entryKey == null) ? 0 : entryKey.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) {
            return true;
         }
         if (obj == null) {
            return false;
         }
         if (!(obj instanceof DeltaCompositeKey)) {
            return false;
         }
         DeltaCompositeKey other = (DeltaCompositeKey) obj;
         if (deltaAwareValueKey == null) {
            if (other.deltaAwareValueKey != null) {
               return false;
            }
         } else if (!deltaAwareValueKey.equals(other.deltaAwareValueKey)) {
            return false;
         }
         if (entryKey == null) {
            if (other.entryKey != null) {
               return false;
            }
         } else if (!entryKey.equals(other.entryKey)) {
            return false;
         }
         return true;
      }

      @Override
      public String toString() {
         //This is used by logger messages when debugging
         return "ApplyDeltaCommand#DeltaCompositeKey[deltaAwareValueKey=" + deltaAwareValueKey + ", entryKey=" + entryKey + ']';
      }
   }
}
