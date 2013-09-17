package org.infinispan.loaders.decorators;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationDoc;
import org.infinispan.config.ConfigurationDocRef;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * SingletonStore is a delegating cache store used for situations when only one 
 * instance in a cluster should interact with the underlying store. The coordinator of the cluster will be responsible for
 * the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore implementation. It always 
 * delegates reads to the real CacheStore.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 * 
 * @see <a href="../../../../config.html#ce_loader_singletonStore">Configuration reference</a>
 */
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
@ConfigurationDoc(name="singletonStore")
@SuppressWarnings("boxing")
public class SingletonStoreConfig extends AbstractDecoratorConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;
   
   @ConfigurationDocRef(bean=SingletonStoreConfig.class,targetElement="setSingletonStoreEnabled")
   protected Boolean enabled = false;
   
   @ConfigurationDocRef(bean=SingletonStoreConfig.class,targetElement="setPushStateWhenCoordinator")
   protected Boolean pushStateWhenCoordinator = true;
   
   @ConfigurationDocRef(bean=SingletonStoreConfig.class,targetElement="setPushStateTimeout")
   protected Long pushStateTimeout = 10000L;

   @XmlAttribute(name="enabled")
   public Boolean isSingletonStoreEnabled() {
      return enabled;
   }

   /**
    * If true, the singleton store cache store is enabled.
    * 
    * @param singletonStoreEnabled
    * @deprecated The visibility of this method will be reduced. Use {@link org.infinispan.loaders.CacheStoreConfig#singletonStore()} instead.
    */
   @Deprecated
   public void setSingletonStoreEnabled(Boolean singletonStoreEnabled) {
      testImmutability("enabled");
      this.enabled = singletonStoreEnabled;
   }
   
   /**
    * If true, the singleton store cache store is enabled.
    * 
    * @param singletonStoreEnabled
    */
   public SingletonStoreConfig enabled(Boolean singletonStoreEnabled) {
      testImmutability("enabled");
      this.enabled = singletonStoreEnabled;
      return this;
   }

   @XmlAttribute
   public Boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    * 
    * @param pushStateWhenCoordinator
    * @deprecated The visibility of this method will be reduced. Use {@link #pushStateWhenCoordinator(Boolean)} instead.
    */
   @Deprecated
   public void setPushStateWhenCoordinator(Boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }
   
   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    * 
    * @param pushStateWhenCoordinator
    */
   public SingletonStoreConfig pushStateWhenCoordinator(Boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
      return this;
   }

   @XmlAttribute
   public Long getPushStateTimeout() {
      return pushStateTimeout;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    * 
    * @param pushStateTimeout
    * @deprecated The visibility of this method will be reduced. Use {@link #pushStateTimeout(Long)} instead.
    */
   @Deprecated
   public void setPushStateTimeout(Long pushStateTimeout) {
      testImmutability("pushStateTimeout");
      this.pushStateTimeout = pushStateTimeout;
   }
   
   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    * 
    * @param pushStateTimeout
    */
   public SingletonStoreConfig pushStateTimeout(Long pushStateTimeout) {
      testImmutability("pushStateTimeout");
      this.pushStateTimeout = pushStateTimeout;
      return this;
   }

   @Override
   public SingletonStoreConfig clone() {
      return (SingletonStoreConfig) super.clone();
   }

   @Override
   public void accept(ConfigurationBeanVisitor v) {
   }
}
