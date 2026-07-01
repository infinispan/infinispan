package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import org.infinispan.commands.write.RemoveAllCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.transport.Address;

class RemoveAllHelper extends WriteManyCommandHelper<RemoveAllCommand, Collection<Object>, Object> {
   RemoveAllHelper(Function<WriteManyCommandHelper<RemoveAllCommand, ?, ?>, InvocationSuccessFunction<RemoveAllCommand>> createRemoteCallback) {
      super(createRemoteCallback);
   }

   @Override
   public RemoveAllCommand copyForLocal(RemoveAllCommand cmd, Collection<Object> container) {
      return new RemoveAllCommand(cmd).withKeys(container);
   }

   @Override
   public RemoveAllCommand copyForPrimary(RemoveAllCommand cmd, LocalizedCacheTopology topology, IntSet segments) {
      return new RemoveAllCommand(cmd).withKeys(new ReadOnlySegmentAwareCollection<>(cmd.getKeys(), topology, segments));
   }

   @Override
   public RemoveAllCommand copyForBackup(RemoveAllCommand cmd, LocalizedCacheTopology topology,
                                         Address target, IntSet segments) {
      RemoveAllCommand copy = new RemoveAllCommand(cmd).withKeys(new ReadOnlySegmentAwareCollection<>(cmd.getKeys(), topology, segments));
      copy.setForwarded(true);
      return copy;
   }

   @Override
   public Collection<Object> getItems(RemoveAllCommand cmd) {
      return cmd.getKeys();
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
   public boolean shouldRegisterRemoteCallback(RemoveAllCommand cmd) {
      return !cmd.isForwarded();
   }

   @Override
   public Object transformResult(Object[] results) {
      return null;
   }
}
