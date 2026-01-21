package org.infinispan.server.resp.commands.connection;

import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.JOLEntrySizeCalculator;

public class MemoryEntrySizeUtils {
   private static JOLEntrySizeCalculator<?, ?> pesc = JOLEntrySizeCalculator.getInstance();
   private static CacheEntrySizeCalculator<byte[], Object> cesc = new CacheEntrySizeCalculator<>(
         JOLEntrySizeCalculator.getInstance());

   public static long calculateSize(byte[] key, InternalCacheEntry<byte[], Object> ice) {
      return cesc.calculateSize(key, ice);
   }

   public static long calculateSize(Object o) {
      return JOLEntrySizeCalculator.getInstance().deepSizeOf(o);
   }
}
