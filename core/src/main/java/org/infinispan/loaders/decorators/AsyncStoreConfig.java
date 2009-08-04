package org.infinispan.loaders.decorators;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.Dynamic;

/**
 * Configuration for the async cache loader
 * 
 * <p>
 * Note that class AsyncStoreConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@ConfigurationElement(name="async", parent="loader")
@XmlAccessorType(XmlAccessType.FIELD)
public class AsyncStoreConfig extends AbstractNamedCacheConfigurationBean {
   @XmlAttribute
   Boolean enabled = false;
  
   @XmlAttribute
   Integer threadPoolSize = 1;
   
   @Dynamic
   @XmlAttribute
   Long mapLockTimeout = 5000L;

   public boolean isEnabled() {
      return enabled;
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "async",
            description="If true, modifications are stored in the cache store asynchronously.")
   public void setEnabled(boolean enabled) {
      testImmutability("enabled");
      this.enabled = enabled;
   }

   public int getThreadPoolSize() {
      return threadPoolSize;
   }
   
   @ConfigurationAttribute(name = "threadPoolSize", 
            containingElement = "async",
            description="Size of the thread pool whose threads are responsible for applying the modifications.")
   public void setThreadPoolSize(int threadPoolSize) {
      testImmutability("threadPoolSize");
      this.threadPoolSize = threadPoolSize;
   }

   public long getMapLockTimeout() {
      return mapLockTimeout;
   }

   @ConfigurationAttribute(name = "mapLockTimeout", 
            containingElement = "async",
            description="Lock timeout for access to map containing latest state.")
   public void setMapLockTimeout(long stateLockTimeout) {
      testImmutability("mapLockTimeout");
      this.mapLockTimeout = stateLockTimeout;
   }   
   
   @Override
   public AsyncStoreConfig clone() {
      try {
         return (AsyncStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
   }
}
