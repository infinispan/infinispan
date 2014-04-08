package org.infinispan.test.concurrent;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;

/**
 * Replaces a cache component with a dynamic proxy that can interact with a {@link StateSequencer} when a method that
 * matches a {@link InvocationMatcher} is called.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class CacheComponentSequencerAction<T> extends GlobalComponentSequencerAction<T> {

   private final Cache<?, ?> cache;

   public CacheComponentSequencerAction(StateSequencer stateSequencer, Cache<?, ?> cache, Class<T> componentClass, InvocationMatcher matcher) {
      super(stateSequencer, cache.getCacheManager(), componentClass, matcher);
      this.cache = cache;
   }

   @Override
   protected void replaceComponent() {
      if (ourHandler == null) {
         T component = cache.getAdvancedCache().getComponentRegistry().getComponent(componentClass);
         if (component == null) {
            throw new IllegalStateException("Attempting to wrap a non-existing component: " + componentClass);
         }
         ourHandler = new ProxyInvocationHandler(component, stateSequencer, matcher);
         T componentProxy = createComponentProxy(componentClass, ourHandler);
         TestingUtil.replaceComponent(cache, componentClass, componentProxy, true);
      }
   }
}
