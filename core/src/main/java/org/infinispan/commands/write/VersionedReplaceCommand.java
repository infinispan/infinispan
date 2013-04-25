/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.commands.Visitor;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.Set;

import static org.infinispan.util.Util.toStr;

/**
 * // TODO: This is a temporary command which will go away in other 5.3 releases
 * // That is because commands should take entire metadata objects rather than individual bits (lifespan, maxIdle, version...)
 * // This is needed to support extra metadata from servers, i.e. MIME for Rest...etc
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class VersionedReplaceCommand extends ReplaceCommand {

   public static final byte COMMAND_ID = 40;

   private EntryVersion version;

   public VersionedReplaceCommand() {
   }

   public VersionedReplaceCommand(Object key, Object oldValue, Object newValue,
         CacheNotifier notifier, long lifespanMillis, long maxIdleTimeMillis,
         EntryVersion version, Set<Flag> flags) {
      super(key, oldValue, newValue, notifier, lifespanMillis, maxIdleTimeMillis, flags);
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
      return visitor.visitVersionedReplaceCommand(ctx, this);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // Perform the regular replace
      Object r = super.perform(ctx);

      // Apply the version to the entry, if conditional operation succeded
      MVCCEntry e = (MVCCEntry) ctx.lookupEntry(key);
      if (e.isChanged())
         e.setVersion(version);

      return r;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{key, oldValue, newValue, lifespanMillis, maxIdleTimeMillis, ignorePreviousValue,
                          Flag.copyWithoutRemotableFlags(flags), previousRead, version};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) throw new IllegalArgumentException("Invalid method name");
      key = parameters[0];
      oldValue = parameters[1];
      newValue = parameters[2];
      lifespanMillis = (Long) parameters[3];
      maxIdleTimeMillis = (Long) parameters[4];
      ignorePreviousValue = (Boolean) parameters[5];
      flags = (Set<Flag>) parameters[6];
      previousRead = (Boolean) parameters[7];
      version = (EntryVersion) parameters[8];
   }

   @Override
   public String toString() {
      return "VersionedReplaceCommand{" +
            "key=" + toStr(key) +
            ", oldValue=" + toStr(oldValue) +
            ", newValue=" + toStr(newValue) +
            ", version=" + version +
            ", lifespanMillis=" + lifespanMillis +
            ", maxIdleTimeMillis=" + maxIdleTimeMillis +
            ", flags=" + flags +
            ", successful=" + successful +
            ", ignorePreviousValue=" + ignorePreviousValue +
            '}';
   }
}
