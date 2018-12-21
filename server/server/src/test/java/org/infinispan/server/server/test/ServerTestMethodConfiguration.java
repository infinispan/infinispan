package org.infinispan.server.server.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ServerTestMethodConfiguration {
   String cacheName() default ""; // Will create a cache name based on the method name

   String cacheConfig() default ""; // The configuration to use for the cache in XML format
}
