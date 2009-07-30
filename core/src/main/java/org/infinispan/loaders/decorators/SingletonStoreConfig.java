package org.infinispan.loaders.decorators;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;

/**
 * Configuration for a singleton store
 * 
 *<p>
 * Note that class SingletonStoreConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 * @version $Id$
 */
@ConfigurationElement(name="singletonStore", parent="loader")
@XmlAccessorType(XmlAccessType.FIELD)
public class SingletonStoreConfig extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;

   @XmlAttribute
   Boolean enabled = false;
   
   @XmlAttribute
   Boolean pushStateWhenCoordinator = true;
   
   @XmlAttribute
   Long pushStateTimeout = 10000L;

   public boolean isSingletonStoreEnabled() {
      return enabled;
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "singletonStore",
            description="Switch to enable singleton store")              
   public void setSingletonStoreEnabled(boolean singletonStoreEnabled) {
      testImmutability("enabled");
      this.enabled = singletonStoreEnabled;
   }


   public boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   @ConfigurationAttribute(name = "pushStateWhenCoordinator", 
            containingElement = "singletonStore",
            description="TODO")
   public void setPushStateWhenCoordinator(boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   public long getPushStateTimeout() {
      return pushStateTimeout;
   }

   @ConfigurationAttribute(name = "pushStateTimeout", 
            containingElement = "singletonStore",
            description="TODO")
   public void setPushStateTimeout(long pushStateTimeout) {
      testImmutability("pushStateTimeout");
      this.pushStateTimeout = pushStateTimeout;
   }

   @Override
   public SingletonStoreConfig clone() {
      try {
         return (SingletonStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen", e);
      }
   }
}
