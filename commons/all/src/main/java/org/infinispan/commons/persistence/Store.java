package org.infinispan.commons.persistence;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Store. An annotation for identifying a persistent store and explicitly stating some of its characteristics.
 *
 * @author Ryan Emerson
 * @since 9.0
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11866. Stores should utilise
 * {@link org.infinispan.persistence.spi.NonBlockingStore.Characteristic}s to specify a store's capabilities.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Deprecated
public @interface Store {
   /**
    * Whether the store can be shared amongst nodes in a distributed/replicated cache
    */
   boolean shared() default false;
}
