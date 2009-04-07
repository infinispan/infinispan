package org.infinispan.loader;

/**
 * Configures individual cache loaders
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheLoaderConfig extends Cloneable {

   CacheLoaderConfig clone();

   String getCacheLoaderClassName();

   void setCacheLoaderClassName(String s);
}
