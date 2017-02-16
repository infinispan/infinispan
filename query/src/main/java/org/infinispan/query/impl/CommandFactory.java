package org.infinispan.query.impl;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.query.affinity.AffinityUpdateCommand;
import org.infinispan.query.clustered.ClusteredQueryCommand;
import org.infinispan.query.indexmanager.IndexUpdateCommand;
import org.infinispan.query.indexmanager.IndexUpdateStreamCommand;
import org.infinispan.util.ByteString;

/**
* Remote commands factory implementation
*
* @author Israel Lacerra <israeldl@gmail.com>
* @since 5.1
*/
public class CommandFactory implements ModuleCommandFactory {

   @Override
   public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
      Map<Byte, Class<? extends ReplicableCommand>> map = new HashMap<Byte, Class<? extends ReplicableCommand>>(1);
      map.put(ClusteredQueryCommand.COMMAND_ID, ClusteredQueryCommand.class);
      map.put(IndexUpdateCommand.COMMAND_ID, IndexUpdateCommand.class);
      map.put(IndexUpdateStreamCommand.COMMAND_ID, IndexUpdateStreamCommand.class);
      map.put(AffinityUpdateCommand.COMMAND_ID, AffinityUpdateCommand.class);
      return map;
   }

   @Override
   public ReplicableCommand fromStream(byte commandId) {
      // Should not be called while this factory only
      // provides cache specific replicable commands.
      return null;
   }

   @Override
   public CacheRpcCommand fromStream(byte commandId, ByteString cacheName) {
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
         case AffinityUpdateCommand.COMMAND_ID:
            c = new AffinityUpdateCommand(cacheName);
            break;
         default:
            throw new IllegalArgumentException("Not registered to handle command id " + commandId);
      }
      return c;
   }

}
