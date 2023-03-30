package org.infinispan.server.memcached;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @since 15.0
 **/
public class MemcachedStats {
   public static final AtomicLongFieldUpdater<MemcachedStats> INCR_MISSES = newUpdater(MemcachedStats.class, "incrMisses");
   public static final AtomicLongFieldUpdater<MemcachedStats> INCR_HITS = newUpdater(MemcachedStats.class, "incrHits");
   public static final AtomicLongFieldUpdater<MemcachedStats> DECR_MISSES = newUpdater(MemcachedStats.class, "decrMisses");
   public static final AtomicLongFieldUpdater<MemcachedStats> DECR_HITS = newUpdater(MemcachedStats.class, "decrHits");
   public static final AtomicLongFieldUpdater<MemcachedStats> CAS_MISSES = newUpdater(MemcachedStats.class, "casMisses");
   public static final AtomicLongFieldUpdater<MemcachedStats> CAS_HITS = newUpdater(MemcachedStats.class, "casHits");
   public static final AtomicLongFieldUpdater<MemcachedStats> CAS_BADVAL = newUpdater(MemcachedStats.class, "casBadval");
   private volatile long incrMisses = 0;
   private volatile long incrHits = 0;
   private volatile long decrMisses = 0;
   private volatile long decrHits = 0;
   private volatile long casMisses = 0;
   private volatile long casHits = 0;
   private volatile long casBadval = 0;
}
