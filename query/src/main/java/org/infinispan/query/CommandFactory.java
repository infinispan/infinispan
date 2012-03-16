/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.query;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.query.clustered.ClusteredQueryCommand;

/**
* Remote commands factory implementation
*
* @author Israel Lacerra <israeldl@gmail.com>
* @since 5.1
*/
public class CommandFactory implements ExtendedModuleCommandFactory {

   @Override
   public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
      Map<Byte, Class<? extends ReplicableCommand>> map = new HashMap<Byte, Class<? extends ReplicableCommand>>(1);
      map.put(ClusteredQueryCommand.COMMAND_ID, ClusteredQueryCommand.class);
      return map;
   }

   @Override
   public ReplicableCommand fromStream(byte commandId, Object[] args) {
      // Should not be called while this factory only
      // provides cache specific replicable commands.
      return null;
   }

   @Override
   public CacheRpcCommand fromStream(byte commandId, Object[] args, String cacheName) {
      CacheRpcCommand c;
      switch (commandId) {
         case ClusteredQueryCommand.COMMAND_ID:
            c = new ClusteredQueryCommand(cacheName);
            break;
         default:
            throw new IllegalArgumentException("Not registered to handle command id " + commandId);
      }
      c.setParameters(commandId, args);
      return c;
   }

}
