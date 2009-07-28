package org.infinispan.loaders;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.infinispan.CacheException;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.PluggableConfigurationComponent;

/**
 * Abstract base class for CacheLoaderConfigs.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class AbstractCacheLoaderConfig extends PluggableConfigurationComponent implements CacheLoaderConfig {

   @XmlAttribute(name="class")
   protected String cacheLoaderClassName;

   public String getCacheLoaderClassName() {
      return cacheLoaderClassName;
   }

   @ConfigurationAttribute(name = "class", 
            containingElement = "loader", 
            description = "Full class name of a cache loader")
   public void setCacheLoaderClassName(String className) {
      if (className == null || className.length() == 0) return;
      testImmutability("cacheLoaderClassName");
      this.cacheLoaderClassName = className;
   }

   @Override
   public AbstractCacheLoaderConfig clone() {
      try {
         return (AbstractCacheLoaderConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new CacheException(e);
      }
   }
}
