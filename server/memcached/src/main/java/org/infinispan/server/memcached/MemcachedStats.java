package org.infinispan.server.memcached;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

import java.nio.charset.StandardCharsets;
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


   public static class MemcachedStatsKeys {

      public static final byte[] PID = "pid".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] UPTIME = "uptime".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] TIME = "time".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] VERSION = "version".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] POINTER_SIZE = "pointer_size".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] RUSAGE_USER = "rusage_user".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] RUSAGE_SYSTEM = "rusage_system".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CURR_ITEMS = "curr_items".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] TOTAL_ITEMS = "total_items".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] BYTES = "bytes".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CMD_GET = "cmd_get".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CMD_SET = "cmd_set".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] GET_HITS = "get_hits".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] GET_MISSES = "get_misses".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] DELETE_MISSES = "delete_misses".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] DELETE_HITS = "delete_hits".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] INCR_MISSES = "incr_misses".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] INCR_HITS = "incr_hits".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] DECR_MISSES = "decr_misses".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] DECR_HITS = "decr_hits".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CAS_MISSES = "cas_misses".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CAS_HITS = "cas_hits".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CAS_BADVAL = "cas_badval".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] AUTH_CMDS = "auth_cmds".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] AUTH_ERRORS = "auth_errors".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] EVICTIONS = "evictions".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] BYTES_READ = "bytes_read".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] BYTES_WRITTEN = "bytes_written".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CURR_CONNECTIONS = "curr_connections".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] TOTAL_CONNECTIONS = "total_connections".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] THREADS = "threads".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CONNECTION_STRUCTURES = "connection_structures".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] LIMIT_MAXBYTES = "limit_maxbytes".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] CONN_YIELDS = "conn_yields".getBytes(StandardCharsets.US_ASCII);
      public static final byte[] RECLAIMED = "reclaimed".getBytes(StandardCharsets.US_ASCII);
   }
}
