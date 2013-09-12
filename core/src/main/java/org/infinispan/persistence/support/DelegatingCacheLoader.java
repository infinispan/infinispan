package org.infinispan.persistence.support;

import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public abstract class DelegatingCacheLoader implements CacheLoader {

   protected CacheLoader actual;
   protected InitializationContext ctx;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      //the delegates only propagate init if the underlaying object is a delegate as well.
      // we do this in order to assure the init is only invoked once on the actual store instance
      if (actual instanceof DelegatingCacheLoader)
         actual.init(ctx);
   }

   @Override
   public void start() {
      if (actual instanceof DelegatingCacheLoader)
         actual.start();
   }

   @Override
   public void stop() {
      if (actual instanceof DelegatingCacheLoader)
         actual.stop();
   }

   protected DelegatingCacheLoader(CacheLoader actual) {
      this.actual = actual;
   }

   @Override
   public boolean contains(Object key) {
      return actual != null && actual.contains(key);
   }

   @Override
   public MarshalledEntry load(Object key) {
      return actual != null ? actual.load(key) : null;
   }

   public CacheLoader undelegate() {
      CacheLoader cl = this;
      do {
         cl = ((DelegatingCacheLoader) cl).actual;
      } while (cl instanceof DelegatingCacheLoader);
      return cl;
   }
}
