/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.commands.write;

import org.infinispan.atomic.Delta;
import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * A form of {@link PutKeyValueCommand} that also applies a version to the entry created.
 *
 * Note that this command is only used during state transfer. Normally versioning requires transactions,
 * and as such it used VersionedPrepareCommand and the regular PutKeyValueCommand.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedPutKeyValueCommand extends PutKeyValueCommand {
   public static final byte COMMAND_ID = 28;
   private EntryVersion version;

   public VersionedPutKeyValueCommand() {
   }

   public VersionedPutKeyValueCommand(Object key, Object value, boolean putIfAbsent, CacheNotifier notifier, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags, EntryVersion version) {
      super(key, value, putIfAbsent, notifier, lifespanMillis, maxIdleTimeMillis, flags);
      this.version = version;
   }

   public EntryVersion getVersion() {
      return version;
   }

   public void setVersion(EntryVersion version) {
      this.version = version;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitVersionedPutKeyValueCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Perform the regular put
      Object r = super.perform(ctx);

      // Apply the version to the entry
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e != null && !(value instanceof Delta)) {
         e.setVersion(version);
      }

      return r;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, value, lifespanMillis, maxIdleTimeMillis, version,
                          Flag.copyWithoutRemotableFlags(flags), previousRead};
   }

   @SuppressWarnings("unchecked")
   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalStateException("Invalid method id");
      key = parameters[0];
      value = parameters[1];
      lifespanMillis = (Long) parameters[2];
      maxIdleTimeMillis = (Long) parameters[3];
      version = (EntryVersion) parameters[4];
      flags = (Set<Flag>) parameters[5];
      previousRead = (Boolean) parameters[6];
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("VersionedPutKeyValueCommand{key=")
            .append(toStr(key))
            .append(", value=").append(toStr(value))
            .append(", version=").append(version)
            .append(", flags=").append(flags)
            .append(", putIfAbsent=").append(putIfAbsent)
            .append(", lifespanMillis=").append(lifespanMillis)
            .append(", maxIdleTimeMillis=").append(maxIdleTimeMillis)
            .append("}")
            .toString();
   }
}
