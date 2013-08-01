package org.infinispan.loaders;

import java.io.Serializable;

import org.infinispan.config.ConfigurationBeanVisitor;

/**
 * Configures individual cache loaders
 *
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Deprecated
public interface CacheLoaderConfig extends Cloneable, Serializable {

   void accept(ConfigurationBeanVisitor visitor);

   CacheLoaderConfig clone();

   String getCacheLoaderClassName();

   void setCacheLoaderClassName(String s);

   /**
    * Get the classloader that should be used to load resources from the classpath
    */
   ClassLoader getClassLoader();
}
