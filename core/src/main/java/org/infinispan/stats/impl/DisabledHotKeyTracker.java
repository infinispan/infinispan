package org.infinispan.stats.impl;

import java.util.List;

import org.infinispan.commons.stat.HeavyKeeper;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.stats.HotKeyTracker;

@Scope(Scopes.NAMED_CACHE)
public final class DisabledHotKeyTracker implements HotKeyTracker {

   private static final HotKeyTracker INSTANCE = new DisabledHotKeyTracker();

   private DisabledHotKeyTracker() { }

   public static HotKeyTracker instance() {
      return INSTANCE;
   }


   @Override
   public void recordRead(Object key, int segment) { }

   @Override
   public void recordWrite(Object key, int segment) { }

   @Override
   public List<HeavyKeeper.KeyFrequency<Object>> getTopReads(int n) {
      return List.of();
   }

   @Override
   public List<HeavyKeeper.KeyFrequency<Object>> getTopWrites(int n) {
      return List.of();
   }

   @Override
   public long totalReads() {
      return 0;
   }

   @Override
   public long totalWrites() {
      return 0;
   }

   @Override
   public long segmentReads(int segment) {
      return 0;
   }

   @Override
   public long segmentWrites(int segment) {
      return 0;
   }

   @Override
   public void reset() { }
}
