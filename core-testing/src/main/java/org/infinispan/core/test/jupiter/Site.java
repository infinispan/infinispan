package org.infinispan.core.test.jupiter;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a site within an {@link InfinispanXSite} cross-site test.
 *
 * @since 16.2
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Site {

   /**
    * Site name (e.g. "LON", "NYC").
    */
   String name();

   /**
    * Number of cache manager nodes in this site.
    */
   int nodes() default 1;
}
