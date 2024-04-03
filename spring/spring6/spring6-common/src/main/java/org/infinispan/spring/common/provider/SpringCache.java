package org.infinispan.spring.common.provider;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.util.NullValue;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link Cache <code>Cache</code>} implementation that delegates to a
 * {@link org.infinispan.Cache <code>org.infinispan.Cache</code>} instance supplied at construction
 * time.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author <a href="mailto:marius.bogoevici@gmail.com">Marius Bogoevici</a>
 */
public class SpringCache implements Cache {
   public static final SimpleValueWrapper NULL_VALUE_WRAPPER = new SimpleValueWrapper(null);

   private final BasicCache nativeCache;
   private final long readTimeout;
   private final long writeTimeout;
   private final Map<Object, ReentrantLock> synchronousGetLocks = new ConcurrentHashMap<>();
   private final Map<Object, CompletableFuture> computationResults = new ConcurrentHashMap<>();

   /**
    * @param nativeCache underlying cache
    */
   public SpringCache(BasicCache nativeCache) {
      this(nativeCache, 0, 0);
   }

   public SpringCache(BasicCache nativeCache, long readTimeout, long writeTimeout) {
      Assert.notNull(nativeCache, "A non-null Infinispan cache implementation is required");
      this.nativeCache = nativeCache;
      this.readTimeout = readTimeout;
      this.writeTimeout = writeTimeout;
   }


   @Override
   public CompletableFuture<?> retrieve(Object key) {
      return encodedToValueWrapper(nativeCache.getAsync(key));
   }
   private CompletableFuture<ValueWrapper> encodedToValueWrapper(CompletableFuture<Object> cf) {
      return cf.thenApply(value -> encodedToValueWrapper(value));
   }

   @Override
   public <T> CompletableFuture<T> retrieve(Object key, Supplier<CompletableFuture<T>> valueLoaderAsync) {
      CompletionStage<T> completionStage = handleAndCompose(nativeCache.getAsync(key), (v1, ex1) -> {
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

         return handleAndCompose(valueLoaderAsync.get(), (newValue, ex2) -> {
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

   private static <T, U> CompletionStage<U> handleAndCompose(CompletionStage<T> stage,
                                                            BiFunction<T, Throwable, CompletionStage<U>> handleFunction) {
      if (isCompletedSuccessfully(stage)) {
         T value = join(stage);
         try {
            return handleFunction.apply(value, null);
         } catch (Throwable t) {
            return completedExceptionFuture(t);
         }
      }
      return stage.handle(handleFunction).thenCompose(Function.identity());
   }

   private static <T> CompletableFuture<T> completedExceptionFuture(Throwable ex) {
      CompletableFuture<T> future = new CompletableFuture<>();
      future.completeExceptionally(ex);
      return future;
   }

   private static boolean isCompletedSuccessfully(CompletionStage<?> stage) {
      CompletableFuture<?> future = stage.toCompletableFuture();
      return future.isDone() && !future.isCompletedExceptionally();
   }

   private static <R> R join(CompletionStage<R> stage) {
      try {
         return await(stage.toCompletableFuture());
      } catch (ExecutionException e) {
         throw new CompletionException(e.getCause());
      } catch (InterruptedException e) {
         throw new CompletionException(e);
      }
   }
   private static final long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);
   private static <T> T await(CompletableFuture<T> future) throws ExecutionException, InterruptedException {
      try {
         return Objects.requireNonNull(future, "Completable Future must be non-null.").get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
         throw new IllegalStateException("This should never happen!", e);
      }
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
   //this exception does not exist. However we can bypass this by using separate class file (which is loaded
   //by the JVM when needed...)
   private static class ValueRetrievalExceptionResolver {
      static RuntimeException throwValueRetrievalException(Object key, Callable<?> loader, Throwable ex) {
         return new ValueRetrievalException(key, loader, ex);
      }
   }

   public long getWriteTimeout() {
      return writeTimeout;
   }
}
