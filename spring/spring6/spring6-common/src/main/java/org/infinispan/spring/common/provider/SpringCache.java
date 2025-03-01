package org.infinispan.spring.common.provider;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.NullValue;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link Cache} implementation that delegates to a
 * {@link BasicCache} instance supplied at construction time.
 * </p>
 *
 * @author Olaf Bergner
 * @author <a href="mailto:marius.bogoevici@gmail.com">Marius Bogoevici</a>
 */
public class SpringCache implements Cache {
   public static final SimpleValueWrapper NULL_VALUE_WRAPPER = new SimpleValueWrapper(null);
   public static final String REACTIVE_DISABLED = "Reactive mode is disabled. " +
           "Enable it by setting 'infinispan.embedded.reactive=true' in embedded mode " +
           "and 'infinispan.remote.reactive=true' in remote mode.";

   private final BasicCache nativeCache;
   private final long readTimeout;
   private final long writeTimeout;
   private final Map<Object, ReentrantLock> synchronousGetLocks = new ConcurrentHashMap<>();
   private final Map<Object, CompletableFuture> computationResults = new ConcurrentHashMap<>();
   private final boolean reactive;

   public SpringCache(BasicCache nativeCache) {
      this(nativeCache, false, 0, 0);
   }

   public SpringCache(BasicCache nativeCache, boolean reactive) {
      this(nativeCache, reactive, 0, 0);
   }

   public SpringCache(BasicCache nativeCache, long readTimeout, long writeTimeout) {
      this(nativeCache, false, readTimeout, writeTimeout);
   }

   public SpringCache(BasicCache nativeCache, boolean reactive, long readTimeout, long writeTimeout) {
      Assert.notNull(nativeCache, "A non-null Infinispan cache implementation is required");
      this.nativeCache = nativeCache;
      this.reactive = reactive;
      this.readTimeout = readTimeout;
      this.writeTimeout = writeTimeout;
   }

   /**
    * @see Cache#getName()
    */
   @Override
   public String getName() {
      return this.nativeCache.getName();
   }

   /**
    * @see Cache#getNativeCache()
    */
   @Override
   public BasicCache<?, ?> getNativeCache() {
      return this.nativeCache;
   }

   /**
    * @see Cache#get(Object)
    */
   @Override
   public ValueWrapper get(final Object key) {
      try {
         if (readTimeout > 0)
            return encodedToValueWrapper(nativeCache.getAsync(key).get(readTimeout, TimeUnit.MILLISECONDS));
         else
            return encodedToValueWrapper(nativeCache.get(key));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public <T> T get(Object key, Class<T> type) {
      try {
         Object value;
         if (readTimeout > 0)
            value = nativeCache.getAsync(key).get(readTimeout, TimeUnit.MILLISECONDS);
         else
            value = nativeCache.get(key);

         value = decodeNull(value);
         if (value != null && type != null && !type.isInstance(value)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + value);
         }
         return (T) value;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }

   }

   @Override
   public <T> T get(Object key, Callable<T> valueLoader) {
      ReentrantLock lock;
      T value = (T) nativeCache.get(key);
      if (value == null) {
         lock = synchronousGetLocks.computeIfAbsent(key, k -> new ReentrantLock());
         lock.lock();
         try {
            if ((value = (T) nativeCache.get(key)) == null) {
               try {
                  T newValue = valueLoader.call();
                  // we can't use computeIfAbsent here since in distributed embedded scenario we would
                  // send a lambda to other nodes. This is the behavior we want to avoid.
                  value = (T) nativeCache.putIfAbsent(key, encodeNull(newValue));
                  if (value == null) {
                     value = newValue;
                  }
               } catch (Exception e) {
                  throw ValueRetrievalExceptionResolver.throwValueRetrievalException(key, valueLoader, e);
               }
            }
         } finally {
            lock.unlock();
            synchronousGetLocks.remove(key);
         }
      }
      return decodeNull(value);
   }

   /**
    * @see Cache#put(Object, Object)
    */
   @Override
   public void put(final Object key, final Object value) {
      if (reactive) {
         this.nativeCache.putAsync(key, encodeNull(value));
         return;
      }

      try {
         if (writeTimeout > 0)
            this.nativeCache.putAsync(key, encodeNull(value)).get(writeTimeout, TimeUnit.MILLISECONDS);
         else
            this.nativeCache.put(key, encodeNull(value));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   /**
    * @see BasicCache#put(Object, Object, long, TimeUnit)
    */
   public void put(Object key, Object value, long lifespan, TimeUnit unit) {
      if (reactive) {
         this.nativeCache.putAsync(key, encodeNull(value), lifespan, unit);
         return;
      }

      try {
         if (writeTimeout > 0)
            this.nativeCache.putAsync(key, encodeNull(value), lifespan, unit).get(writeTimeout, TimeUnit.MILLISECONDS);
         else
            this.nativeCache.put(key, encodeNull(value), lifespan, unit);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public ValueWrapper putIfAbsent(Object key, Object value) {
      try {
         if (writeTimeout > 0)
            return encodedToValueWrapper(this.nativeCache.putIfAbsentAsync(key, encodeNull(value)).get(writeTimeout, TimeUnit.MILLISECONDS));
         else
            return encodedToValueWrapper(this.nativeCache.putIfAbsent(key, encodeNull(value)));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   /**
    * @see Cache#evict(Object)
    */
   @Override
   public void evict(final Object key) {
      if (reactive) {
         this.nativeCache.removeAsync(key);
         return;
      }
      try {
         if (writeTimeout > 0)
            this.nativeCache.removeAsync(key).get(writeTimeout, TimeUnit.MILLISECONDS);
         else
            this.nativeCache.remove(key);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   /**
    * @see Cache#clear()
    */
   @Override
   public void clear() {
      if (reactive) {
         this.nativeCache.clearAsync();
         return;
      }

      try {
         if (writeTimeout > 0)
            this.nativeCache.clearAsync().get(writeTimeout, TimeUnit.MILLISECONDS);
         else
            this.nativeCache.clear();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }


   /**
    * @see Object#toString()
    */
   @Override
   public String toString() {
      return "InfinispanCache [nativeCache = " + this.nativeCache + "]";
   }

   private CompletableFuture<ValueWrapper> encodedToValueWrapper(CompletableFuture<Object> cf) {
      return cf.thenApply(value -> encodedToValueWrapper(value));
   }

   private ValueWrapper encodedToValueWrapper(Object value) {
      if (value == null) {
         return null;
      }
      if (value == NullValue.NULL) {
         return NULL_VALUE_WRAPPER;
      }
      return new SimpleValueWrapper(value);
   }

   private Object encodeNull(Object value) {
      return value != null ? value : NullValue.NULL;
   }

   private <T> T decodeNull(Object value) {
      return value != NullValue.NULL ? (T) value : null;
   }

   //Implemented as a static holder class for backwards compatibility.
   //Imagine a situation where a client has new integration module and old Spring version. In that case
   //this exception does not exist. However, we can bypass this by using separate class file (which is loaded
   //by the JVM when needed...)
   private static class ValueRetrievalExceptionResolver {
      static RuntimeException throwValueRetrievalException(Object key, Callable<?> loader, Throwable ex) {
         return new ValueRetrievalException(key, loader, ex);
      }
   }

   public long getWriteTimeout() {
      return writeTimeout;
   }

   @Override
   public CompletableFuture<?> retrieve(Object key) {
      if (!reactive) {
         throw new UnsupportedOperationException(REACTIVE_DISABLED);
      }
      return encodedToValueWrapper(nativeCache.getAsync(key));
   }

   @Override
   public <T> CompletableFuture<T> retrieve(Object key, Supplier<CompletableFuture<T>> valueLoaderAsync) {
      if (!reactive) {
         throw new UnsupportedOperationException(REACTIVE_DISABLED);
      }

      CompletionStage<T> completionStage = CompletionStages.handleAndCompose(nativeCache.getAsync(key), (v1, ex1) -> {
         if (ex1 != null) {
            return CompletableFuture.failedFuture(ex1);
         }

         if (v1 != null) {
            return CompletableFuture.completedFuture(decodeNull(v1));
         }

         CompletableFuture result = new CompletableFuture<>();
         CompletableFuture<T> computedValue = computationResults.putIfAbsent(key, result);
         if (computedValue != null) {
            return computedValue;
         }

         return CompletionStages.handleAndCompose(valueLoaderAsync.get(), (newValue, ex2) -> {
                     if (ex2 != null) {
                        result.completeExceptionally(ex2);
                        computationResults.remove(key);
                     } else {
                        nativeCache.putIfAbsentAsync(key, encodeNull(newValue))
                              .whenComplete((existing, ex3) -> {
                                 if (ex3 != null) {
                                    result.completeExceptionally((Throwable) ex3);
                                 } else if (existing == null) {
                                    result.complete(newValue);
                                 } else {
                                    result.complete(decodeNull(existing));
                                 }
                                 computationResults.remove(key);
                              });
                     }
                     return result;
                  });
         });
      return (CompletableFuture<T>) completionStage;
   }
}
