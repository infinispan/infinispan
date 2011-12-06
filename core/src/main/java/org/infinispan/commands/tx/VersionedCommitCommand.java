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

package org.infinispan.commands.tx;

import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * The same as a {@link CommitCommand} except that version information is also carried by this command, used by
 * optimistically transactional caches making use of write skew checking when using {@link org.infinispan.util.concurrent.IsolationLevel#REPEATABLE_READ}.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedCommitCommand extends CommitCommand {
   public static final byte COMMAND_ID = 27;
   private EntryVersionsMap updatedVersions;

   public VersionedCommitCommand() {
      super("");
   }

   public VersionedCommitCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public VersionedCommitCommand(String cacheName) {
      super(cacheName);
   }

   public EntryVersionsMap getUpdatedVersions() {
      return updatedVersions;
   }

   public void setUpdatedVersions(EntryVersionsMap updatedVersions) {
      this.updatedVersions = updatedVersions;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, updatedVersions};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      updatedVersions = (EntryVersionsMap) args[1];
   }
}
