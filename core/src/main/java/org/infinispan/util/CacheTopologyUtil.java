package org.infinispan.util;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;

/**
 * Utility methods related to {@link CacheTopology}.
 *
 * @since 14.0
 */
public enum CacheTopologyUtil {
   ;

   private static final long SKIP_TOPOLOGY_FLAGS = FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL;

   /**
    * Check if the current {@link LocalizedCacheTopology} is valid for the {@link TopologyAffectedCommand}.
    *
    * @param command The {@link TopologyAffectedCommand} that will use the {@link LocalizedCacheTopology}.
    * @param current The current {@link LocalizedCacheTopology}.
    * @return The current {@link LocalizedCacheTopology}.
    */
   public static LocalizedCacheTopology checkTopology(TopologyAffectedCommand command, LocalizedCacheTopology current) {
      int currentTopologyId = current.getTopologyId();
      int cmdTopology = command.getTopologyId();
      if (command instanceof FlagAffectedCommand && (((FlagAffectedCommand) command).hasAnyFlag(SKIP_TOPOLOGY_FLAGS))) {
         return current;
      }
      if (cmdTopology >= 0 && currentTopologyId != cmdTopology) {
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }
      return current;
   }
}
