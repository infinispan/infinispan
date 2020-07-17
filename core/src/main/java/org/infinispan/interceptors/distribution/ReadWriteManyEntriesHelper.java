package org.infinispan.interceptors.distribution;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.transport.Address;

class ReadWriteManyEntriesHelper extends WriteManyCommandHelper<ReadWriteManyEntriesCommand, Map<Object, Object>, Map.Entry<Object, Object>> {
   ReadWriteManyEntriesHelper(Function<WriteManyCommandHelper<ReadWriteManyEntriesCommand, ?, ?>, InvocationSuccessFunction<ReadWriteManyEntriesCommand>> createRemoteCallback) {
      super(createRemoteCallback);
   }

   @Override
   public ReadWriteManyEntriesCommand copyForLocal(ReadWriteManyEntriesCommand cmd, Map<Object, Object> entries) {
      return new ReadWriteManyEntriesCommand(cmd).withArguments(entries);
   }

   @Override
   public ReadWriteManyEntriesCommand copyForPrimary(ReadWriteManyEntriesCommand cmd, LocalizedCacheTopology topology, IntSet segments) {
      return new ReadWriteManyEntriesCommand(cmd)
            .withArguments(new ReadOnlySegmentAwareMap<>(cmd.getArguments(), topology, segments));
   }

   @Override
   public ReadWriteManyEntriesCommand copyForBackup(ReadWriteManyEntriesCommand cmd, LocalizedCacheTopology topology,
                                                    Address target, IntSet segments) {
      ReadWriteManyEntriesCommand copy = new ReadWriteManyEntriesCommand(cmd)
            .withArguments(new ReadOnlySegmentAwareMap(cmd.getArguments(), topology, segments));
      copy.setForwarded(true);
      return copy;
   }

   @Override
   public Collection<Map.Entry<Object, Object>> getItems(ReadWriteManyEntriesCommand cmd) {
      return cmd.getArguments().entrySet();
   }

   @Override
   public Object item2key(Map.Entry<Object, Object> entry) {
      return entry.getKey();
   }

   @Override
   public Map<Object, Object> newContainer() {
      // Make sure the iteration in containers is ordered
      return new LinkedHashMap<>();
   }

   @Override
   public void accumulate(Map<Object, Object> map, Map.Entry<Object, Object> entry) {
      map.put(entry.getKey(), entry.getValue());
   }

   @Override
   public int containerSize(Map<Object, Object> map) {
      return map.size();
   }

   @Override
   public Iterable<Object> toKeys(Map<Object, Object> map) {
      return map.keySet();
   }

   @Override
   public boolean shouldRegisterRemoteCallback(ReadWriteManyEntriesCommand cmd) {
      return !cmd.isForwarded();
   }

   @Override
   public Object transformResult(Object[] results) {
      return results == null ? null : Arrays.asList(results);
   }
}
