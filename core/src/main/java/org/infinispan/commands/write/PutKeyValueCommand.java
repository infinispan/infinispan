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

import org.infinispan.Metadata;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#put(Object, Object)}
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PutKeyValueCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   public static final byte COMMAND_ID = 8;

   Object value;
   boolean putIfAbsent;
   CacheNotifier notifier;
   boolean successful = true;
   Metadata metadata;

   public PutKeyValueCommand() {
   }

   public PutKeyValueCommand(Object key, Object value, boolean putIfAbsent,
         CacheNotifier notifier, Metadata metadata, Set<Flag> flags) {
      super(key, flags);
      setValue(value);
      this.putIfAbsent = putIfAbsent;
      this.notifier = notifier;
      this.metadata = metadata;
   }

   public void init(CacheNotifier notifier) {
      this.notifier = notifier;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
      if (value instanceof DeltaAware) {
         setFlags(Flag.DELTA_WRITE);
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPutKeyValueCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      Object o;
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e == null && hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         successful = false;
         return null;
      }
      //possible as in certain situations (e.g. when locking delegation is used) we don't wrap
      if (e == null) return null;

      Object entryValue = e.getValue();
      if (entryValue != null && putIfAbsent && !e.isRemoved()) {
         // Revert assumption that new value is to be committed
         e.setChanged(false);
         successful = false;
         return entryValue;
      } else {
         notifier.notifyCacheEntryModified(
               key, entryValue, entryValue == null, true, ctx, this);

         if (value instanceof Delta) {
            // magic
            Delta dv = (Delta) value;
            DeltaAware toMergeWith = null;
            if (entryValue instanceof DeltaAware) toMergeWith = (DeltaAware) entryValue;
            e.setValue(dv.merge(toMergeWith));
            o = entryValue;
            e.setMetadata(metadata);
         } else {
            o = e.setValue(value);
            if (e.isRemoved()) {
               e.setRemoved(false);
               e.setValid(true);
               o = null;
            }
         }
      }
      return o;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, value, metadata, putIfAbsent, Flag.copyWithoutRemotableFlags(flags), previousRead};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      value = parameters[1];
      metadata = (Metadata) parameters[2];
      putIfAbsent = (Boolean) parameters[3];
      flags = (Set<Flag>) parameters[4];
      previousRead = (Boolean) parameters[5];
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   public boolean isPutIfAbsent() {
      return putIfAbsent;
   }

   public void setPutIfAbsent(boolean putIfAbsent) {
      this.putIfAbsent = putIfAbsent;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      PutKeyValueCommand that = (PutKeyValueCommand) o;

      if (putIfAbsent != that.putIfAbsent) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;
      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (putIfAbsent ? 1 : 0);
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("PutKeyValueCommand{key=")
            .append(toStr(key))
            .append(", value=").append(value)
            .append(", flags=").append(flags)
            .append(", putIfAbsent=").append(putIfAbsent)
            .append(", metadata=").append(metadata)
            .append(", successful=").append(successful)
            .append("}")
            .toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return putIfAbsent;
   }
}