package org.infinispan.spring.provider;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCache;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.Cache <code>Cache</code>} implementation that delegates to a
 * {@link org.infinispan.Cache <code>org.infinispan.Cache</code>} instance supplied at construction
 * time.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author <a href="mailto:marius.bogoevici@gmail.com">Marius Bogoevici</a>
 *
 */
public class SpringCache implements Cache {
   private final BasicCache nativeCache;
   private final long readTimeout;
   private final long writeTimeout;
   private final Map<Object, ReentrantLock> synchronousGetLocks = new ConcurrentHashMap<>();

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

   /**
    * @see org.springframework.cache.Cache#getName()
    */
   @Override
   public String getName() {
      return this.nativeCache.getName();
   }

   /**
    * @see org.springframework.cache.Cache#getNativeCache()
    */
   @Override
   public BasicCache<?, ?> getNativeCache() {
      return this.nativeCache;
   }

   /**
    * @see org.springframework.cache.Cache#get(Object)
    */
   @Override
   public ValueWrapper get(final Object key) {
      try {
         if (readTimeout > 0)
            return wrap(nativeCache.getAsync(key).get(readTimeout, TimeUnit.MILLISECONDS));
         else
            return wrap(nativeCache.get(key));
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
      ReentrantLock lock = null;
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
                  value = (T) nativeCache.putIfAbsent(key, newValue);
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
      return value;
   }

   /**
    * @see org.springframework.cache.Cache#put(Object, Object)
    */
   @Override
   public void put(final Object key, final Object value) {
      try {
         if (writeTimeout > 0)
            this.nativeCache.putAsync(key, value != null ? value : NullValue.NULL).get(writeTimeout, TimeUnit.MILLISECONDS);
         else
            this.nativeCache.put(key, value != null ? value : NullValue.NULL);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   /**
    * @see org.infinispan.commons.api.BasicCache#put(Object, Object, long, TimeUnit)
    */
   public void put(Object key, Object value, long lifespan, TimeUnit unit) {
      try {
         if (writeTimeout > 0)
            this.nativeCache.putAsync(key, value != null ? value : NullValue.NULL, lifespan, unit).get(writeTimeout, TimeUnit.MILLISECONDS);
         else
            this.nativeCache.put(key, value != null ? value : NullValue.NULL, lifespan, unit);
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
            return wrap(this.nativeCache.putIfAbsentAsync(key, value).get(writeTimeout, TimeUnit.MILLISECONDS));
         else
            return wrap(this.nativeCache.putIfAbsent(key, value));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException | TimeoutException e) {
         throw new CacheException(e);
      }
   }

   /**
    * @see org.springframework.cache.Cache#evict(Object)
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
    * @see org.springframework.cache.Cache#clear()
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

   private ValueWrapper wrap(Object value) {
      if (value == null) {
         return null;
      }
      if (value == NullValue.NULL) {
         return NullValue.NULL;
      }
      return new SimpleValueWrapper(value);
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
}
