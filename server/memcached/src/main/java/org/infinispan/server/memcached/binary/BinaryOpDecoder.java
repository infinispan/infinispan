package org.infinispan.server.memcached.binary;

import static org.infinispan.server.memcached.MemcachedStats.CAS_BADVAL;
import static org.infinispan.server.memcached.MemcachedStats.CAS_HITS;
import static org.infinispan.server.memcached.MemcachedStats.CAS_MISSES;
import static org.infinispan.server.memcached.MemcachedStats.DECR_HITS;
import static org.infinispan.server.memcached.MemcachedStats.DECR_MISSES;
import static org.infinispan.server.memcached.MemcachedStats.INCR_HITS;
import static org.infinispan.server.memcached.MemcachedStats.INCR_MISSES;
import static org.infinispan.server.memcached.MemcachedStatus.DELETED;
import static org.infinispan.server.memcached.MemcachedStatus.ITEM_NOT_STORED;
import static org.infinispan.server.memcached.MemcachedStatus.KEY_EXISTS;
import static org.infinispan.server.memcached.MemcachedStatus.KEY_NOT_FOUND;
import static org.infinispan.server.memcached.MemcachedStatus.NO_ERROR;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.infinispan.commons.util.SimpleImmutableEntry;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.memcached.MemcachedMetadata;
import org.infinispan.server.memcached.MemcachedResponse;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.ParseUtil;
import org.infinispan.util.concurrent.CompletionStages;
import org.jgroups.util.CompletableFutures;

abstract class BinaryOpDecoder extends BinaryDecoder {

   protected BinaryOpDecoder(MemcachedServer server, Subject subject) {
      super(server, subject);
   }

   protected MemcachedResponse get(BinaryHeader header, byte[] key, boolean quiet) {
      CompletableFuture<CacheEntry<byte[], byte[]>> cs = cache.getCacheEntryAsync(key);
      if (CompletionStages.isCompletedSuccessfully(cs)) {
         handleGet(CompletionStages.join(cs), header, key, quiet);
         return send(header, CompletableFutures.completedNull());
      }
      return send(header, cs.thenAccept(e -> handleGet(e, header, key, quiet)));
   }

   private void handleGet(CacheEntry<byte[], byte[]> e, BinaryHeader header, byte[] key, boolean quiet) {
      boolean withKey = header.getCommand() == BinaryCommand.GETK || header.getCommand() == BinaryCommand.GETKQ;
      // getq/getkq
      if (e == null) {
         if (!quiet)
            response(header, KEY_NOT_FOUND, withKey ? key : Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY);
      } else {
         MemcachedMetadata metadata = (MemcachedMetadata) e.getMetadata();
         header.setCas(((NumericVersion) metadata.version()).getVersion());
         response(header, NO_ERROR, metadata.flags, withKey ? key : Util.EMPTY_BYTE_ARRAY, e.getValue());
      }
   }

   protected MemcachedResponse set(BinaryHeader header, byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      Metadata metadata = metadata(flags, expiration);

      if (header.getCas() == 0) {
         CompletionStage<?> cs = cache.withFlags(Flag.IGNORE_RETURN_VALUES)
               .putAsync(key, value, metadata);
         if (CompletionStages.isCompletedSuccessfully(cs)) {
            storeResponse(header, quiet, metadata);
            return send(header, CompletableFutures.completedNull());
         }

         return send(header, cs.thenAccept(ignore -> storeResponse(header, quiet, metadata)));
      }

      CompletionStage<CacheEntry<byte[], byte[]>> cs = cache.getCacheEntryAsync(key);
      if (CompletionStages.isCompletedSuccessfully(cs)) {
         return send(header, handleSet(CompletionStages.join(cs), metadata, header, key, value, flags, expiration, quiet));
      }

      CompletableFuture<Void> response = cache.getCacheEntryAsync(key)
            .thenCompose(e -> handleSet(e, metadata, header, key, value, flags, expiration, quiet));
      return send(header, response);
   }

   private CompletionStage<Void> handleSet(CacheEntry<byte[], byte[]> e, Metadata metadata, BinaryHeader header,
                                           byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      if (e == null) {
         response(header, KEY_NOT_FOUND);
         return CompletableFutures.completedNull();
      } else {
         long version = ((NumericVersion) e.getMetadata().version()).getVersion();
         if (version == header.getCas()) {
            return cache.replaceAsync(key, e.getValue(), value, metadata)
                  .thenAccept(ignore -> storeResponse(header, quiet, metadata));
         } else {
            response(header, KEY_EXISTS);
            return CompletableFutures.completedNull();
         }
      }
   }

   protected MemcachedResponse add(BinaryHeader header, byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      Metadata metadata = metadata(flags, expiration);
      CompletableFuture<Void> response = cache
            .putIfAbsentAsyncEntry(key, value, metadata)
            .thenAccept(e -> {
               if (e != null) {
                  response(header, KEY_EXISTS);
               } else {
                  storeResponse(header, quiet, metadata);
               }
            });
      return send(header, response);
   }

   protected MemcachedResponse replace(BinaryHeader header, byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      Metadata metadata = metadata(flags, expiration);
      CompletableFuture<Void> response;
      if (header.getCas() == 0) {
         response = cache
               .replaceAsync(key, value, metadata)
               .thenAccept(e -> {
                  if (e == null) {
                     response(header, KEY_NOT_FOUND);
                  } else {
                     storeResponse(header, quiet, metadata);
                  }
               });
      } else {
         response = cache.getCacheEntryAsync(key).thenCompose(e -> {
            if (e == null) {
               CAS_MISSES.incrementAndGet(statistics);
               response(header, KEY_NOT_FOUND);
               return CompletableFutures.completedNull();
            } else {
               long version = ((NumericVersion) metadata.version()).getVersion();
               if (header.getCas() == version) {
                  return cache.replaceAsync(key, e.getValue(), value, metadata).thenAccept(ignore -> {
                     CAS_HITS.incrementAndGet(statistics);
                     storeResponse(header, quiet, metadata);
                  });
               } else {
                  CAS_BADVAL.incrementAndGet(statistics);
                  response(header, ITEM_NOT_STORED);
                  return CompletableFutures.completedNull();
               }
            }
         });
      }
      return send(header, response);
   }

   private void storeResponse(BinaryHeader header, boolean quiet, Metadata metadata) {
      if (quiet) {
         return;
      }

      header.setCas(((NumericVersion) metadata.version()).getVersion());
      response(header, NO_ERROR);
   }

   protected MemcachedResponse delete(BinaryHeader header, byte[] key, boolean quiet) {
      CompletableFuture<Void> response;
      if (header.getCas() == 0) {
         response = cache.removeAsync(key)
               .thenAccept(v -> {
                  if (v != null && quiet) return;
                  response(header, v == null ? KEY_NOT_FOUND : DELETED);
               });
      } else {
         response = cache.getCacheEntryAsync(key)
               .thenCompose(e -> {
                  if (e == null) {
                     response(header, KEY_NOT_FOUND);
                     return CompletableFutures.completedNull();
                  } else {
                     long version = ((NumericVersion) e.getMetadata().version()).getVersion();
                     if (header.getCas() == version) {
                        return cache.removeAsync(key, e.getValue()).thenAccept(d -> {
                           if (d) {
                              if (!quiet) response(header, DELETED);
                           } else {
                              response(header, KEY_EXISTS);
                           }
                        });
                     } else {
                        response(header, KEY_EXISTS);
                        return CompletableFutures.completedNull();
                     }
                  }
               });
      }
      return send(header, response);
   }

   protected MemcachedResponse increment(BinaryHeader header, byte[] key, long delta, long initial, int expiration, boolean quiet) {
      Metadata metadata = metadata(0, expiration);
      CompletableFuture<byte[]> f;
      if (expiration == -1) {
         f = cache.computeIfPresentAsync(key, (k, v) -> increment(delta, v), metadata);
      } else {
         f = cache.mergeAsync(key, ParseUtil.writeAsciiLong(initial), (v1, v2) -> increment(delta, v1), metadata);
      }
      CompletableFuture<Void> response = f.thenAccept(v -> {
         if (v == null) {
            if (statsEnabled) {
               if (delta > 0) {
                  INCR_MISSES.incrementAndGet(statistics);
               } else {
                  DECR_MISSES.incrementAndGet(statistics);
               }
            }
            response(header, KEY_NOT_FOUND);
            return;
         }
         if (statsEnabled) {
            if (delta > 0) {
               INCR_HITS.incrementAndGet(statistics);
            } else {
               DECR_HITS.incrementAndGet(statistics);
            }
         }
         if (quiet)
            return;
         header.setCas(((NumericVersion) metadata.version()).getVersion());
         response(header, NO_ERROR, ParseUtil.readLong(v));
      });
      return send(header, response);
   }

   private static byte[] increment(long delta, byte[] v1) {
      long l = ParseUtil.readLong(v1);
      l += delta;
      if (l < 0) l = 0;
      return ParseUtil.writeAsciiLong(l);
   }

   protected MemcachedResponse append(BinaryHeader header, byte[] key, byte[] value, boolean quiet) {
      CompletableFuture<Void> response = cache.computeIfPresentAsync(key, (k, v) -> {
         byte[] r = Arrays.copyOf(v, v.length + value.length);
         System.arraycopy(value, 0, r, v.length, value.length);
         return r;
      }, null).thenAccept(v -> {
         if (!quiet) response(header, v == null ? KEY_NOT_FOUND : NO_ERROR);
      });
      return send(header, response);
   }

   protected MemcachedResponse prepend(BinaryHeader header, byte[] key, byte[] value, boolean quiet) {
      CompletableFuture<Void> response = cache.computeIfPresentAsync(key, (k, v) -> {
         byte[] r = Arrays.copyOf(value, v.length + value.length);
         System.arraycopy(v, 0, r, value.length, v.length);
         return r;
      }, null).thenAccept(v -> {
         if (!quiet) response(header, v == null ? KEY_NOT_FOUND : NO_ERROR);
      });
      return send(header, response);
   }

   protected MemcachedResponse quit(BinaryHeader header, boolean quiet) {
      if (quiet) {
         ctx.close();
         return null;
      } else {
         response(header, NO_ERROR);
         return send(header, CompletableFutures.completedNull(), (v) -> ctx.close());
      }
   }

   protected MemcachedResponse version(BinaryHeader header) {
      response(header, NO_ERROR, Version.getVersion().getBytes(StandardCharsets.US_ASCII));
      return send(header, CompletableFutures.completedNull());
   }

   protected MemcachedResponse noop(BinaryHeader header) {
      response(header, NO_ERROR);
      return send(header, CompletableFutures.completedNull());
   }

   protected MemcachedResponse touch(BinaryHeader header, byte[] key, int expiration) {
      CompletableFuture<Void> r = cache.getCacheEntryAsync(key)
            .thenCompose(e -> {
               if (e == null) {
                  response(header, KEY_NOT_FOUND);
                  return CompletableFutures.completedNull();
               } else {
                  return cache.replaceAsync(e.getKey(), e.getValue(), touchMetadata(e, expiration))
                        .thenAccept(ignore -> response(header, NO_ERROR));
               }
            });
      return send(header, r);
   }

   protected MemcachedResponse gat(BinaryHeader header, byte[] key, int expiration, boolean quiet) {
      CompletableFuture<Void> r = cache.getCacheEntryAsync(key)
            .thenCompose(e -> {
               boolean withKey = header.getCommand() == BinaryCommand.GATK || header.getCommand() == BinaryCommand.GATKQ;
               if (e == null) {
                  if (!quiet) response(header, KEY_NOT_FOUND, withKey ? key : Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY);
                  return CompletableFutures.completedNull();
               } else {
                  MemcachedMetadata metadata = (MemcachedMetadata) e.getMetadata();
                  header.setCas(((NumericVersion) metadata.version()).getVersion());
                  return cache.replaceAsync(e.getKey(), e.getValue(), touchMetadata(e, expiration))
                        .thenAccept(ignore -> response(header, NO_ERROR, metadata.flags, withKey ? e.getKey() : Util.EMPTY_BYTE_ARRAY, e.getValue()));
               }
            });
      return send(header, r);
   }

   protected MemcachedResponse stat(BinaryHeader header, byte[] key) {
      CompletionStage<Void> s = server.getBlockingManager().supplyBlocking(() -> {
         Map<byte[], byte[]> map = statsMap();
         if (key != null) {
            if (!map.containsKey(key)) {
               response(header, KEY_NOT_FOUND);
            } else {
               singleStat(header, new SimpleImmutableEntry<>(key, map.get(key)));
            }
         } else {
            for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
               singleStat(header, e);
            }
            response(header, NO_ERROR);
         }
         return null;
      }, "memcached-stats");
      return send(header, s);
   }

   private void singleStat(BinaryHeader header, Map.Entry<byte[], byte[]> e) {
      response(header, NO_ERROR, e.getKey(), e.getValue());
   }

   protected MemcachedResponse flush(BinaryHeader header, int expiration, boolean quiet) {
      final CompletableFuture<Void> future;
      if (expiration == 0) {
         future = cache.clearAsync();
      } else {
         server.getBlockingManager().scheduleRunBlocking(cache::clear, toMillis(expiration), TimeUnit.MILLISECONDS, "memcached-flush");
         future = CompletableFuture.completedFuture(null);
      }
      if (quiet)
         return null;
      return send(header, future.thenAccept(ignore -> response(header, NO_ERROR)));
   }

   protected MemcachedResponse verbosityLevel(BinaryHeader header, int verbosity) {
      response(header, NO_ERROR);
      return send(header, CompletableFutures.completedNull());
   }
}
