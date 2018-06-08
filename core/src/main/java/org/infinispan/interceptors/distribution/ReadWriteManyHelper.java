package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.interceptors.InvocationSuccessFunction;

class ReadWriteManyHelper extends WriteManyCommandHelper<ReadWriteManyCommand, Collection<Object>, Object> {
   ReadWriteManyHelper(Function<WriteManyCommandHelper<ReadWriteManyCommand, ?, ?>, InvocationSuccessFunction> createRemoteCallback) {
      super(createRemoteCallback);
   }

   @Override
   public ReadWriteManyCommand copyForLocal(ReadWriteManyCommand cmd, Collection<Object> keys) {
      return new ReadWriteManyCommand(cmd).withKeys(keys);
   }

   @Override
   public ReadWriteManyCommand copyForPrimary(ReadWriteManyCommand cmd, ConsistentHash ch, IntSet segments) {
      return new ReadWriteManyCommand(cmd).withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
   }

   @Override
   public ReadWriteManyCommand copyForBackup(ReadWriteManyCommand cmd, ConsistentHash ch, IntSet segments) {
      ReadWriteManyCommand copy = new ReadWriteManyCommand(cmd).withKeys(
            new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
      copy.setForwarded(true);
      return copy;
   }

   @Override
   public Collection<Object> getItems(ReadWriteManyCommand cmd) {
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
   public Iterable<Object> toKeys(Collection<Object> list) {
      return list;
   }

   @Override
   public boolean shouldRegisterRemoteCallback(ReadWriteManyCommand cmd) {
      return !cmd.isForwarded();
   }

   @Override
   public Object transformResult(Object[] results) {
      return results == null ? null : Arrays.asList(results);
   }
}
