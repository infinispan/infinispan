package org.infinispan.loaders;

import org.infinispan.CacheException;
import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.PluggableConfigurationComponent;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

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
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class AbstractCacheLoaderConfig extends PluggableConfigurationComponent implements CacheLoaderConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = -4303705423800914433L;

   /** @configRef name="class",desc="Fully qualified name of a cache loader class that must implement 
    *             org.infinispan.loaders.CacheLoader interface." */
   @XmlAttribute(name="class")
   protected String cacheLoaderClassName;

   public String getCacheLoaderClassName() {
      return cacheLoaderClassName;
   }

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

   public void accept(ConfigurationBeanVisitor v) {}
}
