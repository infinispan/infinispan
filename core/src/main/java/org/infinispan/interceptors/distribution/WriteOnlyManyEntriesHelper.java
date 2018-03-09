package org.infinispan.interceptors.distribution;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.interceptors.InvocationSuccessFunction;

class WriteOnlyManyEntriesHelper extends WriteManyCommandHelper<WriteOnlyManyEntriesCommand, Map<Object, Object>, Map.Entry<Object, Object>> {
   WriteOnlyManyEntriesHelper(Function<WriteManyCommandHelper<WriteOnlyManyEntriesCommand, ?, ?>, InvocationSuccessFunction> createRemoteCallback) {
      super(createRemoteCallback);
   }

   @Override
   public WriteOnlyManyEntriesCommand copyForLocal(WriteOnlyManyEntriesCommand cmd, Map<Object, Object> entries) {
      return new WriteOnlyManyEntriesCommand(cmd).withArguments(entries);
   }

   @Override
   public WriteOnlyManyEntriesCommand copyForPrimary(WriteOnlyManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      return new WriteOnlyManyEntriesCommand(cmd)
            .withArguments(new ReadOnlySegmentAwareMap<>(cmd.getArguments(), ch, segments));
   }

   @Override
   public WriteOnlyManyEntriesCommand copyForBackup(WriteOnlyManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      WriteOnlyManyEntriesCommand copy = new WriteOnlyManyEntriesCommand(cmd)
            .withArguments(new ReadOnlySegmentAwareMap(cmd.getArguments(), ch, segments));
      copy.setForwarded(true);
      return copy;
   }

   @Override
   public Collection<Map.Entry<Object, Object>> getItems(WriteOnlyManyEntriesCommand cmd) {
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
   public boolean isForwarded(WriteOnlyManyEntriesCommand cmd) {
      return cmd.isForwarded();
   }

   @Override
   public void setForwarded(WriteOnlyManyEntriesCommand backupCommand) {
      backupCommand.setForwarded(true);
   }

   @Override
   public Object transformResult(Object[] results) {
      return results == null ? null : Arrays.asList(results);
   }

   @Override
   public Collection<Map.Entry<Object, Object>> asCollection(Map<Object, Object> map) {
      return map.entrySet();
   }
}
