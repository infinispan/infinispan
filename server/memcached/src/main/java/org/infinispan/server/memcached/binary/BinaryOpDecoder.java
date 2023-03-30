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
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.memcached.MemcachedMetadata;
import org.infinispan.server.memcached.MemcachedServer;
import org.jgroups.util.CompletableFutures;

import io.netty.buffer.ByteBuf;

abstract class BinaryOpDecoder extends BinaryDecoder {

   protected BinaryOpDecoder(MemcachedServer server, Subject subject) {
      super(server, subject);
   }

   protected void get(BinaryHeader header, byte[] key, boolean quiet) {
      CompletableFuture<ByteBuf> response = cache.getCacheEntryAsync(key).thenApply(e -> {
         boolean withKey = header.op == BinaryCommand.GETK || header.op == BinaryCommand.GETKQ;
         // getq/getkq
         if (e == null) {
            if (quiet)
               return null;
            return response(header, KEY_NOT_FOUND, withKey ? key : Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY);
         } else {
            MemcachedMetadata metadata = (MemcachedMetadata) e.getMetadata();
            header.cas = ((NumericVersion) metadata.version()).getVersion();
            return response(header, NO_ERROR, metadata.flags, withKey ? key : Util.EMPTY_BYTE_ARRAY, e.getValue());
         }
      });
      send(header, response);
   }

   protected void set(BinaryHeader header, byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      Metadata metadata = metadata(flags, expiration);
      CompletableFuture<ByteBuf> response;
      if (header.cas == 0) {
         response = cache.withFlags(Flag.IGNORE_RETURN_VALUES)
               .putAsync(key, value, metadata)
               .thenApply(ignore -> storeResponse(header, quiet, metadata));
      } else {
         response = cache.getCacheEntryAsync(key).thenCompose(e -> {
            if (e == null) {
               return CompletableFuture.completedFuture(response(header, KEY_NOT_FOUND));
            } else {
               long version = ((NumericVersion) e.getMetadata().version()).getVersion();
               if (version == header.cas) {
                  return cache.replaceAsync(key, e.getValue(), value, metadata).thenApply(ignore -> storeResponse(header, quiet, metadata));
               } else {
                  return CompletableFuture.completedFuture(response(header, KEY_EXISTS));
               }
            }
         });
      }
      send(header, response);
   }

   protected void add(BinaryHeader header, byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      Metadata metadata = metadata(flags, expiration);
      CompletableFuture<ByteBuf> response = cache
            .putIfAbsentAsyncEntry(key, value, metadata)
            .thenApply(e -> {
               if (e != null) {
                  return response(header, KEY_EXISTS);
               } else {
                  return storeResponse(header, quiet, metadata);
               }
            });
      send(header, response);
   }

   protected void replace(BinaryHeader header, byte[] key, byte[] value, int flags, int expiration, boolean quiet) {
      Metadata metadata = metadata(flags, expiration);
      CompletableFuture<ByteBuf> response;
      if (header.cas == 0) {
         response = cache
               .replaceAsync(key, value, metadata)
               .thenApply(e -> {
                  if (e == null) {
                     return response(header, KEY_NOT_FOUND);
                  } else {
                     return storeResponse(header, quiet, metadata);
                  }
               });
      } else {
         response = cache.getCacheEntryAsync(key).thenCompose(e -> {
            if (e == null) {
               CAS_MISSES.incrementAndGet(statistics);
               return CompletableFuture.completedFuture(response(header, KEY_NOT_FOUND));
            } else {
               long version = ((NumericVersion) metadata.version()).getVersion();
               if (header.cas == version) {
                  return cache.replaceAsync(key, e.getValue(), value, metadata).thenApply(ignore -> {
                     CAS_HITS.incrementAndGet(statistics);
                     return storeResponse(header, quiet, metadata);
                  });
               } else {
                  CAS_BADVAL.incrementAndGet(statistics);
                  return CompletableFuture.completedFuture(response(header, ITEM_NOT_STORED));
               }
            }
         });
      }
      send(header, response);
   }

   private ByteBuf storeResponse(BinaryHeader header, boolean quiet, Metadata metadata) {
      if (quiet) {
         return null;
      }
      header.cas = ((NumericVersion) metadata.version()).getVersion();
      return response(header, NO_ERROR);
   }

   protected void delete(BinaryHeader header, byte[] key, boolean quiet) {
      CompletableFuture<ByteBuf> response;
      if (header.cas == 0) {
         response = cache.removeAsync(key)
               .thenApply(v -> {
                  if (v != null && quiet) {
                     return null;
                  } else {
                     return response(header, v == null ? KEY_NOT_FOUND : DELETED);
                  }
               });
      } else {
         response = cache.getCacheEntryAsync(key)
               .thenCompose(e -> {
                  if (e == null) {
                     return CompletableFuture.completedFuture(response(header, KEY_NOT_FOUND));
                  } else {
                     long version = ((NumericVersion) e.getMetadata().version()).getVersion();
                     if (header.cas == version) {
                        return cache.removeAsync(key, e.getValue()).thenApply(d -> {
                           if (d) {
                              return quiet ? null : response(header, DELETED);
                           } else {
                              return response(header, KEY_EXISTS);
                           }
                        });
                     } else {
                        return CompletableFuture.completedFuture(response(header, KEY_EXISTS));
                     }
                  }
               });
      }
      send(header, response);
   }

   protected void increment(BinaryHeader header, byte[] key, long delta, long initial, int expiration, boolean quiet) {
      Metadata metadata = metadata(0, expiration);
      CompletableFuture<byte[]> f;
      if (expiration == -1) {
         f = cache.computeIfPresentAsync(key, (k, v) -> increment(delta, v), metadata);
      } else {
         f = cache.mergeAsync(key, Long.toString(initial).getBytes(StandardCharsets.US_ASCII), (v1, v2) -> increment(delta, v1), metadata);
      }
      CompletableFuture<ByteBuf> response = f.thenApply(v -> {
         if (v == null) {
            if (statsEnabled) {
               if (delta > 0) {
                  INCR_MISSES.incrementAndGet(statistics);
               } else {
                  DECR_MISSES.incrementAndGet(statistics);
               }
            }
            return response(header, KEY_NOT_FOUND);
         }
         if (statsEnabled) {
            if (delta > 0) {
               INCR_HITS.incrementAndGet(statistics);
            } else {
               DECR_HITS.incrementAndGet(statistics);
            }
         }
         if (quiet)
            return null;
         header.cas = ((NumericVersion) metadata.version()).getVersion();
         return response(header, NO_ERROR, new String(v));
      });
      send(header, response);
   }

   private static byte[] increment(long delta, byte[] v1) {
      long l = Long.parseLong(new String(v1));
      l += delta;
      if (l < 0) l = 0;
      return Long.toString(l).getBytes(StandardCharsets.US_ASCII);
   }

   protected void append(BinaryHeader header, byte[] key, byte[] value, boolean quiet) {
      CompletableFuture<ByteBuf> response = cache.computeIfPresentAsync(key, (k, v) -> {
         byte[] r = Arrays.copyOf(v, v.length + value.length);
         System.arraycopy(value, 0, r, v.length, value.length);
         return r;
      }, null).thenApply(v -> {
         if (quiet)
            return null;
         return response(header, v == null ? KEY_NOT_FOUND : NO_ERROR);
      });
      send(header, response);
   }

   protected void prepend(BinaryHeader header, byte[] key, byte[] value, boolean quiet) {
      CompletableFuture<ByteBuf> response = cache.computeIfPresentAsync(key, (k, v) -> {
         byte[] r = Arrays.copyOf(value, v.length + value.length);
         System.arraycopy(v, 0, r, value.length, v.length);
         return r;
      }, null).thenApply(v -> {
         if (quiet)
            return null;
         return response(header, v == null ? KEY_NOT_FOUND : NO_ERROR);
      });
      send(header, response);
   }

   protected void quit(BinaryHeader header, boolean quiet) {
      if (quiet) {
         channel.close();
      } else {
         ByteBuf buf = response(header, NO_ERROR);
         send(header, CompletableFuture.completedFuture(buf), (v) -> channel.close());
      }
   }

   protected void version(BinaryHeader header) {
      ByteBuf r = response(header, NO_ERROR, Version.getVersion().getBytes(StandardCharsets.US_ASCII));
      send(header, CompletableFuture.completedFuture(r));
   }

   protected void noop(BinaryHeader header) {
      send(header, CompletableFuture.completedFuture(response(header, NO_ERROR)));
   }

   protected void touch(BinaryHeader header, byte[] key, int expiration) {
      CompletableFuture<Object> r = cache.getCacheEntryAsync(key)
            .thenCompose(e -> {
               if (e == null) {
                  return CompletableFuture.completedFuture(response(header, KEY_NOT_FOUND));
               } else {
                  return cache.replaceAsync(e.getKey(), e.getValue(), touchMetadata(e, expiration))
                        .thenApply(ignore -> response(header, NO_ERROR));
               }
            });
      send(header, r);
   }

   protected void gat(BinaryHeader header, byte[] key, int expiration, boolean quiet) {
      CompletableFuture<Object> r = cache.getCacheEntryAsync(key)
            .thenCompose(e -> {
               boolean withKey = header.op == BinaryCommand.GATK || header.op == BinaryCommand.GATKQ;
               if (e == null) {
                  if (quiet)
                     return CompletableFutures.completedNull();
                  else
                     return CompletableFuture.completedFuture(response(header, KEY_NOT_FOUND, withKey ? key : Util.EMPTY_BYTE_ARRAY, Util.EMPTY_BYTE_ARRAY));
               } else {
                  MemcachedMetadata metadata = (MemcachedMetadata) e.getMetadata();
                  header.cas = ((NumericVersion) metadata.version()).getVersion();
                  return cache.replaceAsync(e.getKey(), e.getValue(), touchMetadata(e, expiration))
                        .thenApply(x -> response(header, NO_ERROR, metadata.flags, withKey ? e.getKey() : Util.EMPTY_BYTE_ARRAY, e.getValue()));
               }
            });
      send(header, r);
   }

   protected void stat(BinaryHeader header, byte[] key) {
      CompletionStage<ByteBuf> s = server.getBlockingManager().supplyBlocking(() -> {
         Map<String, String> map = statsMap();
         if (key != null) {
            String skey = new String(key, StandardCharsets.US_ASCII);
            if (!map.containsKey(skey)) {
               return response(header, KEY_NOT_FOUND);
            } else {
               return singleStat(header, new SimpleImmutableEntry<>(skey, map.get(skey)));
            }
         } else {
            ByteBuf buf = channel.alloc().buffer();
            for (Map.Entry<String, String> e : map.entrySet()) {
               buf.writeBytes(singleStat(header, e));
            }
            buf.writeBytes(response(header, NO_ERROR));
            return buf;
         }
      }, "memcached-stats");
      send(header, s);
   }

   private ByteBuf singleStat(BinaryHeader header, Map.Entry<String, String> e) {
      return response(header, NO_ERROR, e.getKey().getBytes(StandardCharsets.US_ASCII), e.getValue().getBytes(StandardCharsets.US_ASCII));
   }

   protected void flush(BinaryHeader header, int expiration, boolean quiet) {
      final CompletableFuture<Void> future;
      if (expiration == 0) {
         future = cache.clearAsync();
      } else {
         server.getBlockingManager().scheduleRunBlocking(cache::clear, toMillis(expiration), TimeUnit.MILLISECONDS, "memcached-flush");
         future = CompletableFuture.completedFuture(null);
      }
      if (quiet)
         return;
      send(header, future.thenApply(ignore -> response(header, NO_ERROR)));
   }

   protected void verbosityLevel(BinaryHeader header, int verbosity) {
      send(header, CompletableFuture.completedFuture(response(header, NO_ERROR)));
   }
}
