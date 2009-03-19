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
package org.horizon.commands;

import org.horizon.commands.read.GetKeyValueCommand;
import org.horizon.commands.read.SizeCommand;
import org.horizon.commands.remote.ReplicateCommand;
import org.horizon.commands.tx.CommitCommand;
import org.horizon.commands.tx.PrepareCommand;
import org.horizon.commands.tx.RollbackCommand;
import org.horizon.commands.write.ClearCommand;
import org.horizon.commands.write.EvictCommand;
import org.horizon.commands.write.InvalidateCommand;
import org.horizon.commands.write.PutKeyValueCommand;
import org.horizon.commands.write.PutMapCommand;
import org.horizon.commands.write.RemoveCommand;
import org.horizon.commands.write.ReplaceCommand;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.remoting.transport.Address;
import org.horizon.transaction.GlobalTransaction;

import java.util.List;
import java.util.Map;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface CommandsFactory {

   PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value);

   PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, long lifespanMillis);

   RemoveCommand buildRemoveCommand(Object key, Object value);

   InvalidateCommand buildInvalidateCommand(Object... keys);

   ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue);

   ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, long lifespanMillis);

   SizeCommand buildSizeCommand();

   GetKeyValueCommand buildGetKeyValueCommand(Object key);

   PutMapCommand buildPutMapCommand(Map t);

   PutMapCommand buildPutMapCommand(Map t, long lifespanMillis);

   ClearCommand buildClearCommand();

   EvictCommand buildEvictCommand(Object key);

   PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List modifications, Address localAddress, boolean onePhaseCommit);

   CommitCommand buildCommitCommand(GlobalTransaction gtx);

   RollbackCommand buildRollbackCommand(GlobalTransaction gtx);

   /**
    * Initializes a {@link org.horizon.commands.ReplicableCommand} read from a data stream with components specific to
    * the target cache instance.
    * <p/>
    * Implementations should also be deep, in that if the command contains other commands, these should be recursed
    * into.
    * <p/>
    *
    * @param command command to initialize.  Cannot be null.
    */
   void initializeReplicableCommand(ReplicableCommand command);

   ReplicateCommand buildReplicateCommand(List<ReplicableCommand> toReplicate);

   ReplicateCommand buildReplicateCommand(ReplicableCommand call);
}
