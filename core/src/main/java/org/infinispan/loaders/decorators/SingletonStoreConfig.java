package org.infinispan.loaders.decorators;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;

/**
 * Configuration for a singleton store
 * 
 *<p>
 * Note that class SingletonStoreConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 * @configRef singletonStore
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class SingletonStoreConfig extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;

   @XmlAttribute
   protected Boolean enabled = false;
   
   @XmlAttribute
   protected Boolean pushStateWhenCoordinator = true;
   
   @XmlAttribute
   protected Long pushStateTimeout = 10000L;

   public boolean isSingletonStoreEnabled() {
      return enabled;
   }
   
   public void setSingletonStoreEnabled(boolean singletonStoreEnabled) {
      testImmutability("enabled");
      this.enabled = singletonStoreEnabled;
   }


   public boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   public void setPushStateWhenCoordinator(boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   public long getPushStateTimeout() {
      return pushStateTimeout;
   }

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
