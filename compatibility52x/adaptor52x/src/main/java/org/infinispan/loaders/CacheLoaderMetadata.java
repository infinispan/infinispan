package org.infinispan.loaders;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows you to provide metadata, such as CacheLoaderConfig type via annotation so that the CacheLoader or CacheStore
 * need not be instantiated by the configuration parser to set up the cache loader configuration.
 * <p />
 * This annotation is not necessary, since {@link org.infinispan.loaders.CacheLoader#getConfigurationClass()} still
 * needs to be implemented and serves the same purpose.  It is, however, a runtime optimization.
 *
 * @author Manik Surtani
 * @since 4.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface CacheLoaderMetadata {
   Class<? extends CacheLoaderConfig> configurationClass();
}
