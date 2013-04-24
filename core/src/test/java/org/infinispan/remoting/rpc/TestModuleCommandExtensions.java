/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.remoting.rpc;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;

import java.util.HashMap;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class TestModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ExtendedModuleCommandFactory getModuleCommandFactory() {
      return new ExtendedModuleCommandFactory() {
         @Override
         public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
            Map<Byte, Class<? extends ReplicableCommand>> map = new HashMap<Byte, Class<? extends ReplicableCommand>>(2);
            map.put(CustomReplicableCommand.COMMAND_ID, CustomReplicableCommand.class);
            map.put(CustomCacheRpcCommand.COMMAND_ID, CustomCacheRpcCommand.class);
            return map;
         }

         @Override
         public ReplicableCommand fromStream(byte commandId, Object[] args) {
            ReplicableCommand c;
            switch (commandId) {
               case CustomReplicableCommand.COMMAND_ID:
                  c = new CustomReplicableCommand();
                  break;
               default:
                  throw new IllegalArgumentException("Not registered to handle command id " + commandId);
            }
            c.setParameters(commandId, args);
            return c;
         }

         @Override
         public CacheRpcCommand fromStream(byte commandId, Object[] args, String cacheName) {
            CacheRpcCommand c;
            switch (commandId) {
               case CustomCacheRpcCommand.COMMAND_ID:
                  c = new CustomCacheRpcCommand(cacheName);
                  break;
               default:
                  throw new IllegalArgumentException("Not registered to handle command id " + commandId);
            }
            c.setParameters(commandId, args);
            return c;
         }
      };
   }

   @Override
   public ModuleCommandInitializer getModuleCommandInitializer() {
      return new ModuleCommandInitializer() {
         @Override
         public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
            // nothing to do here
         }
      };
   }
}
