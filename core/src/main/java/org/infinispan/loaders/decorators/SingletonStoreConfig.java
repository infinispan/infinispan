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
 * @configRef singletonStore|SingletonStore is a delegating cache store used for situations when only one 
 * instance should interact with the underlying store. The coordinator of the cluster will be responsible for 
 * the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore implementation. It always 
 * delegates reads to the real CacheStore.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class SingletonStoreConfig extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;

   /**
    *  @configRef |If true, the relevant cache store is turned into singleton store
    *  */
   @XmlAttribute
   protected Boolean enabled = false;
   
   /**
    *  @configRef |If true and the node becomes the coordinator, the in-memory state transfer 
    *  to the underlying cache store is initiated
    *  */
   @XmlAttribute
   protected Boolean pushStateWhenCoordinator = true;
   
   /**
    *  @configRef |If pushStateWhenCoordinator is true, the in-memory state transfer to cache store timeout
    *  */
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
