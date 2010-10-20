package org.infinispan.loaders;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.infinispan.CacheException;
import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationDocRef;
import org.infinispan.config.PluggableConfigurationComponent;

/**
 * Abstract base class for CacheLoaderConfigs.
 * 
 *
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @since 4.0
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class AbstractCacheLoaderConfig extends PluggableConfigurationComponent implements CacheLoaderConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = -4303705423800914433L;

   @XmlAttribute(name="class")
   @ConfigurationDocRef(name="class", bean=AbstractCacheLoaderConfig.class,targetElement="setCacheLoaderClassName")
   protected String cacheLoaderClassName;

   public String getCacheLoaderClassName() {
      return cacheLoaderClassName;
   }

   /** 
    * Fully qualified name of a cache loader class that must implement 
    *             org.infinispan.loaders.CacheLoader interface
    * 
    * @see org.infinispan.loaders.CacheLoaderConfig#setCacheLoaderClassName(java.lang.String)
    */
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
