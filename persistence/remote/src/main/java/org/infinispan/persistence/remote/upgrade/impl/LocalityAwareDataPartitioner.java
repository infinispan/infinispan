package org.infinispan.persistence.remote.upgrade.impl;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.persistence.remote.upgrade.DataPartitioner;

/**
 * The default {@link DataPartitioner} where each partition contains only segments that are local to a server.
 */
public class LocalityAwareDataPartitioner implements DataPartitioner {

   @Override
   public Collection<Set<Integer>> split(CacheTopologyInfo sourceClusterTopology, int partitionsPerServer) {
      Map<SocketAddress, Set<Integer>> segmentsPerServer = sourceClusterTopology.getSegmentsPerServer();
      int numServers = segmentsPerServer.size();
      int numSegments = sourceClusterTopology.getNumSegments();

      if (partitionsPerServer == 0 || numSegments == 0 || numServers == 0) return Collections.emptySet();

      Set<Integer> segmentsQueue = IntStream.range(0, numSegments).boxed().collect(Collectors.toSet());

      if (partitionsPerServer == 1 && numServers == 1) return Collections.singleton(segmentsQueue);

      List<List<Integer>> result = new ArrayList<>();
      for (int i = 0; i < numServers; i++) {
         result.add(new ArrayList<>());
      }

      List<Set<Integer>> sourceSegments = new ArrayList<>(segmentsPerServer.values());

      int serverCursor = 0;
      while (!segmentsQueue.isEmpty()) {
         Set<Integer> ownedSegments = sourceSegments.get(serverCursor);
         Optional<Integer> collected = segmentsQueue.stream().filter(ownedSegments::contains).findFirst();
         if (collected.isPresent()) {
            Integer collectedSegment = collected.get();
            segmentsQueue.remove(collectedSegment);
            result.get(serverCursor).add(collectedSegment);
         }
         if (++serverCursor == numServers) serverCursor = 0;
      }
      return result.stream().flatMap(l -> split(l, partitionsPerServer).stream()).collect(Collectors.toSet());
   }

   /**
    * Splits a list into parts trying to keep each part with the same amount of elements.
    */
   private <T> Set<Set<T>> split(List<T> list, final int parts) {
      List<List<T>> subLists = new ArrayList<>(parts);
      for (int i = 0; i < parts; i++) {
         subLists.add(new ArrayList<>());
      }
      for (int i = 0; i < list.size(); i++) {
         subLists.get(i % parts).add(list.get(i));
      }
      return subLists.stream().filter(l -> !l.isEmpty()).map(HashSet::new).collect(Collectors.toSet());
   }

}
