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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.Visitor;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;


/**
 * @author Vladimir Blagojevic
 * @since 5.1
 */
public class ApplyDeltaCommand extends AbstractDataWriteCommand {

   public static final int COMMAND_ID = 25;

   private Collection<Object> keys;
   private Delta delta;

   public ApplyDeltaCommand() {
   }

   public ApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection<Object> keys) {
      super(deltaAwareValueKey, EnumSet.of(Flag.DELTA_WRITE));

      if (keys == null || keys.isEmpty())
         throw new IllegalArgumentException("At least one key to be locked needs to be specified");

      this.keys = keys;
      this.delta = delta;
   }

   public Delta getDelta(){
      return delta;
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
      return "ApplyDeltaCommand[key=" + key + ", delta=" + delta + ", keys=" + keys+ ']';
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, delta, keys, Flag.copyWithoutRemotableFlags(flags)};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      // TODO: Check duplicated in all commands? A better solution is needed.
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Unsupported command id:" + commandId);
      int i = 0;
      key = args[i++];
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

   public Object[] getCompositeKeys() {
      DeltaCompositeKey[] compositeKeys = new DeltaCompositeKey[keys.size()];
      int i = 0;
      for (Object k : keys) {
         compositeKeys[i++] = new DeltaCompositeKey(key, k);
      }
      return compositeKeys;
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
      return keys.equals(that.keys);
   }

   @Override
   public int hashCode() {
      return 31 * super.hashCode() + keys.hashCode();
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
    */
   private static final class DeltaCompositeKey {

      private final Object deltaAwareValueKey;
      private final Object entryKey;

      public DeltaCompositeKey(Object deltaAwareValueKey, Object entryKey) {
         if (deltaAwareValueKey == null || entryKey == null)
            throw new IllegalArgumentException("Keys cannot be null");

         this.deltaAwareValueKey = deltaAwareValueKey;
         this.entryKey = entryKey;
      }

      @Override
      public int hashCode() {
         return 31 * deltaAwareValueKey.hashCode() + entryKey.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) {
            return true;
         }
         if (!(obj instanceof DeltaCompositeKey)) {
            return false;
         }
         DeltaCompositeKey other = (DeltaCompositeKey) obj;
         return deltaAwareValueKey.equals(other.deltaAwareValueKey) && entryKey.equals(other.entryKey);
      }

      @Override
      public String toString() {
         return "DeltaCompositeKey[deltaAwareValueKey=" + deltaAwareValueKey + ", entryKey=" + entryKey + ']';
      }
   }

   public static class DeltaCompositeKeyExternalizer extends AbstractExternalizer<DeltaCompositeKey> {

      @Override
      public void writeObject(ObjectOutput output, DeltaCompositeKey dck) throws IOException {
         output.writeObject(dck.deltaAwareValueKey);
         output.writeObject(dck.entryKey);
      }

      @Override
      @SuppressWarnings("unchecked")
      public DeltaCompositeKey readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         Object deltaAwareValueKey = unmarshaller.readObject();
         Object entryKey = unmarshaller.readObject();
         return new DeltaCompositeKey(deltaAwareValueKey, entryKey);
      }

      @Override
      public Integer getId() {
         return Ids.DELTA_COMPOSITE_KEY;
      }

      @Override
      public Set<Class<? extends DeltaCompositeKey>> getTypeClasses() {
         return Collections.<Class<? extends DeltaCompositeKey>>singleton(DeltaCompositeKey.class);
      }
   }
}
