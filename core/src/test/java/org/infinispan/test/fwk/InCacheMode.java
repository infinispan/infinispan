package org.infinispan.test.fwk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.infinispan.configuration.cache.CacheMode;

/**
 * Mark which cache modes should the test be executed upon. This will be used to fill in
 * {@link org.infinispan.test.MultipleCacheManagersTest#cacheMode} for each of the modes.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
public @interface InCacheMode {
   CacheMode[] value();
}
