package org.infinispan.interceptors.distribution;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.interceptors.InvocationSuccessFunction;

class PutMapHelper extends WriteManyCommandHelper<PutMapCommand, Map<Object, Object>, Map.Entry<Object, Object>> {
   PutMapHelper(Function<WriteManyCommandHelper<PutMapCommand, ?, ?>, InvocationSuccessFunction> createRemoteCallback) {
      super(createRemoteCallback);
   }

   @Override
   public PutMapCommand copyForLocal(PutMapCommand cmd, Map<Object, Object> container) {
      return new PutMapCommand(cmd).withMap(container);
   }

   @Override
   public PutMapCommand copyForPrimary(PutMapCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      return new PutMapCommand(cmd).withMap(new ReadOnlySegmentAwareMap<>(cmd.getMap(), ch, segments));
   }

   @Override
   public PutMapCommand copyForBackup(PutMapCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      PutMapCommand copy = new PutMapCommand(cmd).withMap(new ReadOnlySegmentAwareMap(cmd.getMap(), ch, segments));
      copy.setForwarded(true);
      return copy;
   }

   @Override
   public Collection<Map.Entry<Object, Object>> getItems(PutMapCommand cmd) {
      return cmd.getMap().entrySet();
   }

   @Override
   public Object item2key(Map.Entry<Object, Object> entry) {
      return entry.getKey();
   }

   @Override
   public Map<Object, Object> newContainer() {
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
   public boolean shouldRegisterRemoteCallback(PutMapCommand cmd) {
      return !cmd.isForwarded();
   }

   @Override
   public Object transformResult(Object[] results) {
      if (results == null) return null;
      Map<Object, Object> result = new HashMap<>();
      for (Object r : results) {
         Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) r;
         result.put(entry.getKey(), entry.getValue());
      }
      return result;
   }
}
