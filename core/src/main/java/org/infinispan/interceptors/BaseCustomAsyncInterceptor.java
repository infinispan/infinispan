package org.infinispan.interceptors;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Anyone using the {@link AsyncInterceptorChain#addInterceptor(AsyncInterceptor, int)} method (or any of its
 * overloaded forms) or registering custom interceptors via XML should extend this base class when creating their own
 * custom interceptors.
 * <p />
 * Annotations on custom interceptors, including {@link Inject}, {@link Start} and {@link Stop}
 * will not be respected and callbacks will not be made.
 * <p />
 * Instead, custom interceptor authors should extend this base class to gain access to {@link Cache} and {@link EmbeddedCacheManager},
 * from which other components may be accessed.  Further, lifecycle should be implemented by overriding {@link #start()}
 * and {@link #stop()} as defined in this class.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class BaseCustomAsyncInterceptor extends DDAsyncInterceptor {
   @Inject private ComponentRef<Cache<?, ?>> cacheRef;
   @Inject protected EmbeddedCacheManager embeddedCacheManager;

   protected Cache<?, ?> cache;

   @Start(priority = 1)
   private void setup() {
      // Needed for backwards compatibility
      this.cache = cacheRef.wired();
   }

   @Start
   protected void start() {
      // Meant to be overridden
   }

   @Stop
   protected void stop() {
      // Meant to be overridden
   }
}
