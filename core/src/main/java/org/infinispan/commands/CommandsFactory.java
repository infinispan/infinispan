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
package org.infinispan.commands;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A factory to build commands, initializing and injecting dependencies accordingly.  Commands built for a specific,
 * named cache instance cannot be reused on a different cache instance since most commands contain the cache name it
 * was built for along with references to other named-cache scoped components.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface CommandsFactory {

   /**
    * Builds a PutKeyValueCommand
    * @param key key to put
    * @param value value to put
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @return a PutKeyValueCommand
    */
   PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, long lifespanMillis, long maxIdleTimeMillis);

   /**
    * Builds a RemoveCommand
    * @param key key to remove
    * @param value value to check for ina  conditional remove, or null for an unconditional remove.
    * @return a RemoveCommand
    */
   RemoveCommand buildRemoveCommand(Object key, Object value);

   /**
    * Builds an InvalidateCommand
    * @param keys keys to invalidate
    * @return an InvalidateCommand
    */
   InvalidateCommand buildInvalidateCommand(Object... keys);

   /**
    * Builds an InvalidateFromL1Command
    * @param forRehash set to true if the invalidation is happening due to a new node taking ownership.  False if it is due to a write, changing the state of the entry.
    * @param keys keys to invalidate
    * @return an InvalidateFromL1Command
    */
   InvalidateCommand buildInvalidateFromL1Command(boolean forRehash, Object... keys);

   /**
    * Builds a ReplaceCommand
    * @param key key to replace
    * @param oldValue existing value to check for if conditional, null if unconditional.
    * @param newValue value to replace with
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @return a ReplaceCommand
    */
   ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, long lifespanMillis, long maxIdleTimeMillis);

   /**
    * Builds a SizeCommand
    * @return a SizeCommand
    */
   SizeCommand buildSizeCommand();

   /**
    * Builds a GetKeyValueCommand
    * @param key key to get
    * @return a GetKeyValueCommand
    */
   GetKeyValueCommand buildGetKeyValueCommand(Object key);

   /**
    * Builds a KeySetCommand
    * @return a KeySetCommand
    */
   KeySetCommand buildKeySetCommand();

   /**
    * Builds a ValuesCommand
    * @return a ValuesCommand
    */
   ValuesCommand buildValuesCommand();

   /**
    * Builds a EntrySetCommand
    * @return a EntrySetCommand
    */
   EntrySetCommand buildEntrySetCommand();

   /**
    * Builds a PutMapCommand
    * @param map map containing key/value entries to put
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @return a PutMapCommand
    */
   PutMapCommand buildPutMapCommand(Map map, long lifespanMillis, long maxIdleTimeMillis);

   /**
    * Builds a ClearCommand
    * @return a ClearCommand
    */
   ClearCommand buildClearCommand();

   /**
    * Builds an EvictCommand
    * @param key key to evict
    * @return an EvictCommand
    */
   EvictCommand buildEvictCommand(Object key);

   /**
    * Builds a PrepareCommand
    * @param gtx global transaction associated with the prepare
    * @param modifications list of modifications
    * @param onePhaseCommit is this a one-phase or two-phase transaction?
    * @return a PrepareCommand
    */
   PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit);

   /**
    * Builds a CommitCommand
    * @param gtx global transaction associated with the commit
    * @return a CommitCommand
    */
   CommitCommand buildCommitCommand(GlobalTransaction gtx);

   /**
    * Builds a RollbackCommand
    * @param gtx global transaction associated with the rollback
    * @return a RollbackCommand
    */
   RollbackCommand buildRollbackCommand(GlobalTransaction gtx);

   /**
    * Initializes a {@link org.infinispan.commands.ReplicableCommand} read from a data stream with components specific
    * to the target cache instance.
    * <p/>
    * Implementations should also be deep, in that if the command contains other commands, these should be recursed
    * into.
    * <p/>
    *
    * @param command command to initialize.  Cannot be null.
    */
   void initializeReplicableCommand(ReplicableCommand command);

   /**
    * Builds an RpcCommand "envelope" containing multiple ReplicableCommands
    * @param toReplicate ReplicableCommands to include in the envelope
    * @return a MultipleRpcCommand
    */
   MultipleRpcCommand buildReplicateCommand(List<ReplicableCommand> toReplicate);

   /**
    * Builds a SingleRpcCommand "envelope" containing a single ReplicableCommand
    * @param call ReplicableCommand to include in the envelope
    * @return a SingleRpcCommand
    */
   SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call);

   /**
    * Builds a StateTransferControlCommand
    * @param block whether to start blocking or not
    * @return a StateTransferControlCommand
    */
   StateTransferControlCommand buildStateTransferControlCommand(boolean block);

   /**
    * Builds a ClusteredGetCommand, which is a remote lookup command
    * @param key key to look up
    * @return a ClusteredGetCommand
    */
   ClusteredGetCommand buildClusteredGetCommand(Object key);

   /**
    * Builds a LockControlCommand to control explicit remote locking
    * @param keys keys to lock
    * @param implicit whether the lock command was implicit (triggered internally) or explicit (triggered by an API call)
    * @return a LockControlCommand
    */
   LockControlCommand buildLockControlCommand(Collection keys, boolean implicit);

   /**
    * Builds a RehashControlCommand for coordinating a rehash event.  This version of this factory method creates a simple
    * control command with just a command type and sender.
    * @param subtype type of RehashControlCommand
    * @param sender sender's Address
    * @return a RehashControlCommand
    */
   RehashControlCommand buildRehashControlCommand(RehashControlCommand.Type subtype, Address sender);

   /**
    * Builds a RehashControlCommand for coordinating a rehash event.  This version of this factory method creates a
    * control command with a sender and a payload - a map of state to be pushed to the recipient.  The {@link org.infinispan.commands.control.RehashControlCommand.Type}
    * of this command is {@link org.infinispan.commands.control.RehashControlCommand.Type#PUSH_STATE}.
    *
    * @param sender sender's Address
    * @param state state map to be pushed to the recipient of this command
    * @return a RehashControlCommand
    */
   RehashControlCommand buildRehashControlCommand(Address sender, Map<Object, InternalCacheValue> state);

   /**
    * Builds a RehashControlCommand for coordinating a rehash event.  This version of this factory method creates a
    * control command with a sender and a payload - a transaction log of writes that occured during the generation and
    * delivery of state.  The {@link org.infinispan.commands.control.RehashControlCommand.Type}
    * of this command is {@link org.infinispan.commands.control.RehashControlCommand.Type#DRAIN_TX}.
    *
    * @param sender sender's Address
    * @param state list of writes
    * @return a RehashControlCommand
    */
   RehashControlCommand buildRehashControlCommandTxLog(Address sender, List<WriteCommand> state);

   /**
    * Builds a RehashControlCommand for coordinating a rehash event.  This version of this factory method creates a
    * control command with a sender and a payload - a transaction log of pending prepares that occured during the generation
    * and delivery of state.  The {@link org.infinispan.commands.control.RehashControlCommand.Type}
    * of this command is {@link org.infinispan.commands.control.RehashControlCommand.Type#DRAIN_TX_PREPARES}.
    *
    * @param sender sender's Address
    * @param state list of pending prepares
    * @return a RehashControlCommand
    */
   RehashControlCommand buildRehashControlCommandTxLogPendingPrepares(Address sender, List<PrepareCommand> state);


   /**
    * Builds a RehashControlCommand for coordinating a rehash event. This particular variation of RehashControlCommand
    * coordinates rehashing of nodes when a node join or leaves
    * 
    * @param subtype
    * @param sender
    * @param state
    * @param oldCH
    * @param leaversHandled
    * @param newCH
    * @return
    */
   RehashControlCommand buildRehashControlCommand(RehashControlCommand.Type subtype,
            Address sender, Map<Object, InternalCacheValue> state, ConsistentHash oldCH,
            ConsistentHash newCH, List<Address> leaversHandled);
}
