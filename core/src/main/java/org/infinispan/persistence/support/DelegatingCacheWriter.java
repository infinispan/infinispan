package org.infinispan.persistence.support;

import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public abstract class DelegatingCacheWriter implements CacheWriter {

   protected final CacheWriter actual;
   protected InitializationContext ctx;

   public DelegatingCacheWriter(CacheWriter actual) {
      this.actual = actual;
   }

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      //the delegates only propagate init if the underlaying object is a delegate as well.
      // we do this in order to assure the init is only invoked once
      if (actual instanceof DelegatingCacheWriter)
         actual.init(ctx);
   }

   @Override
   public void start() {
      if (actual instanceof DelegatingCacheWriter)
         actual.start();
   }

   @Override
   public void stop() {
      if (actual instanceof DelegatingCacheWriter)
         actual.stop();
   }

   @Override
   public void write(MarshalledEntry entry) {
      actual.write(entry);
   }

   @Override
   public boolean delete(Object key) {
      return actual.delete(key);
   }

   public CacheWriter undelegate() {
      CacheWriter cl = this;
      do {
         cl = ((DelegatingCacheWriter) cl).actual;
      } while (cl instanceof DelegatingCacheWriter);
      return cl;
   }

}
