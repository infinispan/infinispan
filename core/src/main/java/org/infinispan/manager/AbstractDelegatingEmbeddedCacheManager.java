package org.infinispan.manager;

/**
 * @deprecated Extend from
 * {@link org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager}
 * instead. This class will be removed in the future.
 */
@Deprecated
public class AbstractDelegatingEmbeddedCacheManager extends org.infinispan.manager.impl.AbstractDelegatingEmbeddedCacheManager {
   public AbstractDelegatingEmbeddedCacheManager(EmbeddedCacheManager cm) {
      super(cm);
   }
}
