package org.infinispan.loaders.decorators;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationBeanVisitor;

/**
 * Configuration for a singleton store
 * 
 *<p>
 * Note that class SingletonStoreConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 * @configRef name="singletonStore",desc="SingletonStore is a delegating cache store used for situations when only one 
 * instance should interact with the underlying store. The coordinator of the cluster will be responsible for 
 * the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore implementation. It always 
 * delegates reads to the real CacheStore."
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class SingletonStoreConfig extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;

   /** @configRef desc="If true, the singleton store cache store is enabled. "*/
   protected Boolean enabled = false;
   
   /** @configRef desc="If true, when a node becomes the coordinator, it will transfer in-memory state to the 
    *             underlying cache store. This can be very useful in situations where the coordinator crashes and 
    *             there's a gap in time until the new coordinator is elected." */
   protected Boolean pushStateWhenCoordinator = true;
   
   /** @configRef desc="If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds that 
    *             the process of pushing the in-memory state to the underlying cache loader should take." */
   protected Long pushStateTimeout = 10000L;

   @XmlAttribute(name="enabled")
   public Boolean isSingletonStoreEnabled() {
      return enabled;
   }

   public void setSingletonStoreEnabled(Boolean singletonStoreEnabled) {
      testImmutability("enabled");
      this.enabled = singletonStoreEnabled;
   }

   @XmlAttribute
   public Boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   public void setPushStateWhenCoordinator(Boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   @XmlAttribute
   public Long getPushStateTimeout() {
      return pushStateTimeout;
   }

   public void setPushStateTimeout(Long pushStateTimeout) {
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

   public void accept(ConfigurationBeanVisitor v) {
      v.visitSingletonStoreConfig(this);
   }
}
