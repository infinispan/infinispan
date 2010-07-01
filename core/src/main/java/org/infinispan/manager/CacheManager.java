package org.infinispan.manager;

/**
 * This interface is only for backward compatibility with Infinispan 4.0.Final and it will be removed in a future version.
 * Use {@link org.infinispan.manager.EmbeddedCacheManager} or {@link CacheContainer}
 * wherever needed.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Deprecated public interface CacheManager extends EmbeddedCacheManager {
}
