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
 * <p>
 * Note that class AbstractCacheLoaderConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 * @author Mircea.Markus@jboss.com
 * @autor Vladimir Blagojevic
 * @since 4.0
 * @version $Id$
 * 
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
