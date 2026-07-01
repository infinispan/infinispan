package org.infinispan.server.memcached;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.infinispan.commons.marshall.WrappedByteArray;

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


   public static class MemcachedStatsKeys {

      public static final WrappedByteArray PID = new WrappedByteArray("pid".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray UPTIME = new WrappedByteArray("uptime".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray TIME = new WrappedByteArray("time".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray VERSION = new WrappedByteArray("version".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray POINTER_SIZE = new WrappedByteArray("pointer_size".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray RUSAGE_USER = new WrappedByteArray("rusage_user".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray RUSAGE_SYSTEM = new WrappedByteArray("rusage_system".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CURR_ITEMS = new WrappedByteArray("curr_items".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray TOTAL_ITEMS = new WrappedByteArray("total_items".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray BYTES = new WrappedByteArray("bytes".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CMD_GET = new WrappedByteArray("cmd_get".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CMD_SET = new WrappedByteArray("cmd_set".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray GET_HITS = new WrappedByteArray("get_hits".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray GET_MISSES = new WrappedByteArray("get_misses".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray DELETE_MISSES = new WrappedByteArray("delete_misses".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray DELETE_HITS = new WrappedByteArray("delete_hits".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray INCR_MISSES = new WrappedByteArray("incr_misses".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray INCR_HITS = new WrappedByteArray("incr_hits".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray DECR_MISSES = new WrappedByteArray("decr_misses".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray DECR_HITS = new WrappedByteArray("decr_hits".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CAS_MISSES = new WrappedByteArray("cas_misses".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CAS_HITS = new WrappedByteArray("cas_hits".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CAS_BADVAL = new WrappedByteArray("cas_badval".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray AUTH_CMDS = new WrappedByteArray("auth_cmds".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray AUTH_ERRORS = new WrappedByteArray("auth_errors".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray EVICTIONS = new WrappedByteArray("evictions".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray BYTES_READ = new WrappedByteArray("bytes_read".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray BYTES_WRITTEN = new WrappedByteArray("bytes_written".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CURR_CONNECTIONS = new WrappedByteArray("curr_connections".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray TOTAL_CONNECTIONS = new WrappedByteArray("total_connections".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray THREADS = new WrappedByteArray("threads".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CONNECTION_STRUCTURES = new WrappedByteArray("connection_structures".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray LIMIT_MAXBYTES = new WrappedByteArray("limit_maxbytes".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray CONN_YIELDS = new WrappedByteArray("conn_yields".getBytes(StandardCharsets.US_ASCII));
      public static final WrappedByteArray RECLAIMED = new WrappedByteArray("reclaimed".getBytes(StandardCharsets.US_ASCII));
   }
}
