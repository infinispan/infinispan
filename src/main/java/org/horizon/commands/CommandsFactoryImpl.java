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

import org.horizon.Cache;
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
import org.horizon.container.DataContainer;
import org.horizon.factories.annotations.Inject;
import org.horizon.notifications.cachelistener.CacheNotifier;
import org.horizon.remoting.transport.Address;
import org.horizon.transaction.GlobalTransaction;

import java.util.List;
import java.util.Map;

/**
 * @author Mircea.Markus@jboss.com
 * @since 1.0
 */
public class CommandsFactoryImpl implements CommandsFactory {
   private DataContainer dataContainer;
   private CacheNotifier notifier;
   private Cache cache;

   // some stateless commands can be reused so that they aren't constructed again all the time.
   SizeCommand cachedSizeCommand;

   @Inject
   public void setupDependencies(DataContainer container, CacheNotifier notifier, Cache cache) {
      this.dataContainer = container;
      this.notifier = notifier;
      this.cache = cache;
   }

   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value) {
      return new PutKeyValueCommand(key, value, false, notifier);
   }

   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, long lifespanMillis) {
      return new PutKeyValueCommand(key, value, false, notifier, lifespanMillis);
   }

   public RemoveCommand buildRemoveCommand(Object key, Object value) {
      return new RemoveCommand(key, value, notifier);
   }

   public InvalidateCommand buildInvalidateCommand(Object... keys) {
      return new InvalidateCommand(notifier, keys);
   }

   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue) {
      return new ReplaceCommand(key, oldValue, newValue);
   }

   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, long lifespan) {
      return new ReplaceCommand(key, oldValue, newValue, lifespan);
   }

   public SizeCommand buildSizeCommand() {
      if (cachedSizeCommand == null) {
         cachedSizeCommand = new SizeCommand(dataContainer);
      }
      return cachedSizeCommand;
   }

   public GetKeyValueCommand buildGetKeyValueCommand(Object key) {
      return new GetKeyValueCommand(key, notifier);
   }

   public PutMapCommand buildPutMapCommand(Map map) {
      return new PutMapCommand(map, notifier);
   }

   public PutMapCommand buildPutMapCommand(Map map, long lifespan) {
      return new PutMapCommand(map, notifier, lifespan);
   }

   public ClearCommand buildClearCommand() {
      return new ClearCommand();
   }

   public EvictCommand buildEvictCommand(Object key) {
      EvictCommand command = new EvictCommand(key, notifier);
      return command;
   }

   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List modifications, Address localAddress, boolean onePhaseCommit) {
      return new PrepareCommand(gtx, modifications, localAddress, onePhaseCommit);
   }

   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return new CommitCommand(gtx);
   }

   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return new RollbackCommand(gtx);
   }

   public ReplicateCommand buildReplicateCommand(List<ReplicableCommand> toReplicate) {
      return new ReplicateCommand(toReplicate, cache.getName());
   }

   public ReplicateCommand buildReplicateCommand(ReplicableCommand call) {
      return new ReplicateCommand(call, cache.getName());
   }

   public void initializeReplicableCommand(ReplicableCommand c) {
      if (c == null) return;
      switch (c.getCommandId()) {
         case PutKeyValueCommand.METHOD_ID:
            ((PutKeyValueCommand) c).init(notifier);
            break;
         case PutMapCommand.METHOD_ID:
            ((PutMapCommand) c).init(notifier);
            break;
         case RemoveCommand.METHOD_ID:
            ((RemoveCommand) c).init(notifier);
            break;
         case ReplicateCommand.METHOD_ID:
            ReplicateCommand rc = (ReplicateCommand) c;
            if (rc.getCommands() != null)
               for (ReplicableCommand nested : rc.getCommands()) initializeReplicableCommand(nested);
            break;
         case InvalidateCommand.METHOD_ID:
            InvalidateCommand ic = (InvalidateCommand) c;
            ic.init(notifier);
            break;
         case PrepareCommand.METHOD_ID:
            PrepareCommand pc = (PrepareCommand) c;
            if (pc.getModifications() != null)
               for (ReplicableCommand nested : pc.getModifications()) initializeReplicableCommand(nested);
            break;
      }
   }
}
