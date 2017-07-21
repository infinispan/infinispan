package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.interceptors.InvocationSuccessFunction;

class WriteOnlyManyHelper extends WriteManyCommandHelper<WriteOnlyManyCommand, Collection<Object>, Object> {
   WriteOnlyManyHelper(Function<WriteManyCommandHelper<WriteOnlyManyCommand, ?, ?>, InvocationSuccessFunction> createRemoteCallback) {
      super(createRemoteCallback);
   }

   @Override
   public WriteOnlyManyCommand copyForLocal(WriteOnlyManyCommand cmd, Collection<Object> keys) {
      return new WriteOnlyManyCommand(cmd).withKeys(keys);
   }

   @Override
   public WriteOnlyManyCommand copyForPrimary(WriteOnlyManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      return new WriteOnlyManyCommand(cmd)
            .withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
   }

   @Override
   public WriteOnlyManyCommand copyForBackup(WriteOnlyManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      WriteOnlyManyCommand copy = new WriteOnlyManyCommand(cmd)
            .withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
      copy.setForwarded(true);
      return copy;
   }

   @Override
   public Collection<Object> getItems(WriteOnlyManyCommand cmd) {
      return cmd.getAffectedKeys();
   }

   @Override
   public Object item2key(Object key) {
      return key;
   }

   @Override
   public Collection<Object> newContainer() {
      return new ArrayList<>();
   }

   @Override
   public void accumulate(Collection<Object> list, Object key) {
      list.add(key);
   }

   @Override
   public int containerSize(Collection<Object> list) {
      return list.size();
   }

   @Override
   public boolean shouldRegisterRemoteCallback(WriteOnlyManyCommand cmd) {
      return !cmd.isForwarded();
   }

   @Override
   public Object transformResult(Object[] results) {
      return results == null ? null : Arrays.asList(results);
   }
}
