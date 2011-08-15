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
package org.infinispan.marshall.exts;

import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.ModuleProperties;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

import static org.infinispan.util.ModuleProperties.moduleOnlyReplicableCommands;

/**
 * ReplicableCommandExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ReplicableCommandExternalizer extends AbstractExternalizer<ReplicableCommand> {
   RemoteCommandsFactory cmdFactory;
   
   public void inject(RemoteCommandsFactory cmdFactory) {
      this.cmdFactory = cmdFactory;
   }

   @Override
   public void writeObject(ObjectOutput output, ReplicableCommand command) throws IOException {
      writeCommandHeader(output, command);
      writeCommandParameters(output, command);
   }

   protected void writeCommandParameters(ObjectOutput output, ReplicableCommand command) throws IOException {
      Object[] args = command.getParameters();
      int numArgs = (args == null ? 0 : args.length);

      UnsignedNumeric.writeUnsignedInt(output, numArgs);
      for (int i = 0; i < numArgs; i++) {
         Object arg = args[i];
         if (arg instanceof DeltaAware) {
            // Only write deltas so that replication can be more efficient
            DeltaAware dw = (DeltaAware) arg;
            output.writeObject(dw.delta());
         } else {
            output.writeObject(arg);
         }
      }
   }

   protected void writeCommandHeader(ObjectOutput output, ReplicableCommand command) throws IOException {
      // To decide whether it's a core or user defined command, load them all and check
      Collection<Class<? extends ReplicableCommand>> moduleCommands = getModuleCommands();
      // Write an indexer to separate commands defined external to the
      // infinispan core module from the ones defined via module commands
      if (moduleCommands.contains(command.getClass()))
         output.writeByte(1);
      else
         output.writeByte(0);

      output.writeShort(command.getCommandId());
   }

   @Override
   public ReplicableCommand readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      byte type = input.readByte();
      short methodId = input.readShort();
      Object[] args = readParameters(input);
      return cmdFactory.fromStream((byte) methodId, args, type);
   }

   protected Object[] readParameters(ObjectInput input) throws IOException, ClassNotFoundException {
      int numArgs = UnsignedNumeric.readUnsignedInt(input);
      Object[] args = null;
      if (numArgs > 0) {
         args = new Object[numArgs];
         // For DeltaAware instances, nothing special to be done here.
         // Do not merge here since the cache contents are required.
         // Instead, merge in PutKeyValueCommand.perform
         for (int i = 0; i < numArgs; i++) args[i] = input.readObject();
      }
      return args;
   }

   @Override
   public Integer getId() {
      return Ids.REPLICABLE_COMMAND;
   }

   @Override
   public Set<Class<? extends ReplicableCommand>> getTypeClasses() {
       Set<Class<? extends ReplicableCommand>> coreCommands = Util.<Class<? extends ReplicableCommand>>asSet(
            DistributedExecuteCommand.class, GetKeyValueCommand.class,
            ClearCommand.class, EvictCommand.class,
            InvalidateCommand.class, InvalidateL1Command.class,
            PutKeyValueCommand.class, PutMapCommand.class,
            RemoveCommand.class, ReplaceCommand.class);
      // Search only those commands that replicable and not cache specific replicable commands
      Collection<Class<? extends ReplicableCommand>> moduleCommands = moduleOnlyReplicableCommands();
      if (moduleCommands != null && !moduleCommands.isEmpty()) coreCommands.addAll(moduleCommands);
      return coreCommands;
   }

   private Collection<Class<? extends ReplicableCommand>> getModuleCommands() {
      return ModuleProperties.moduleCommands(null);
   }

}