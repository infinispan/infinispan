package org.infinispan.query.impl;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.query.clustered.ClusteredQueryCommand;
import org.infinispan.query.indexmanager.IndexUpdateCommand;
import org.infinispan.query.indexmanager.IndexUpdateStreamCommand;

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
      map.put(Byte.valueOf(ClusteredQueryCommand.COMMAND_ID), ClusteredQueryCommand.class);
      map.put(Byte.valueOf(IndexUpdateCommand.COMMAND_ID), IndexUpdateCommand.class);
      map.put(Byte.valueOf(IndexUpdateStreamCommand.COMMAND_ID), IndexUpdateStreamCommand.class);
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
         case IndexUpdateCommand.COMMAND_ID:
            c = new IndexUpdateCommand(cacheName);
            break;
         case IndexUpdateStreamCommand.COMMAND_ID:
            c = new IndexUpdateStreamCommand(cacheName);
            break;
         default:
            throw new IllegalArgumentException("Not registered to handle command id " + commandId);
      }
      c.setParameters(commandId, args);
      return c;
   }

}
