package org.infinispan.config;

import org.infinispan.commons.util.Util;
import org.infinispan.config.FluentConfiguration.LoadersConfig;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import java.util.LinkedList;
import java.util.List;

/**
 * Holds the configuration of the cache loader chain. All cache loaders should be defined using this
 * class, adding individual cache loaders to the chain by calling
 *
 * 
 *
 * 
 * @see <a href="../../../config.html#ce_default_loaders">Configuration reference</a>
 * 
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Brian Stansberry
 * @author Vladimir Blagojevic
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.PROPERTY)
@ConfigurationDoc(name="loaders",desc="Holds the configuration for cache loaders and stores")
@SuppressWarnings("boxing")
public class CacheLoaderManagerConfig extends AbstractFluentConfigurationBean implements LoadersConfig {

   private static final long serialVersionUID = 2210349340378984424L;

   @ConfigurationDocRef(bean=CacheLoaderManagerConfig.class,targetElement="setPassivation")
   protected Boolean passivation = false;

   @ConfigurationDocRef(bean=CacheLoaderManagerConfig.class,targetElement="setPreload")   
   protected Boolean preload = false;

   @ConfigurationDocRef(bean=CacheLoaderManagerConfig.class,targetElement="setShared")   
   protected Boolean shared = false;
  
   public CacheLoaderManagerConfig() {
   }

   public Boolean isPreload() {
      return preload;
   }

   /**
    * If true, when the cache starts, data stored in the cache store will be pre-loaded into memory.
    * This is particularly useful when data in the cache store will be needed immediately after
    * startup and you want to avoid cache operations being delayed as a result of loading this data
    * lazily. Can be used to provide a 'warm-cache' on startup, however there is a performance
    * penalty as startup time is affected by this process.
    * 
    * @param preload
    */
   @Override
   public LoadersConfig preload(Boolean preload) {
      testImmutability("preload");
      this.preload = preload;
      return this;
   }

   /**
    * @deprecated The visibility of this method will be reduced. Use {@link #preload(Boolean)} instead.
    */
   @XmlAttribute
   @Deprecated
   public void setPreload(Boolean preload) {
      testImmutability("preload");
      this.preload = preload;
   }

   /**
    * If true, data is only written to the cache store when it is evicted from memory, a phenomenon
    * known as 'passivation'. Next time the data is requested, it will be 'activated' which means
    * that data will be brought back to memory and removed from the persistent store. This gives you
    * the ability to 'overflow' to disk, similar to swapping in an operating system. <br />
    * <br />
    * If false, the cache store contains a copy of the contents in memory, so writes to cache result
    * in cache store writes. This essentially gives you a 'write-through' configuration.
    * 
    * @param passivation
    */   
   @Override
   public LoadersConfig passivation(Boolean passivation) {
      testImmutability("passivation");
      this.passivation = passivation;
      return this;
   }
   
   /**
    * @deprecated The visibility of this method will be reduced. Use {@link #passivation(Boolean)} instead.
    */
   @XmlAttribute
   @Deprecated
   public void setPassivation(Boolean passivation) {
      testImmutability("passivation");
      this.passivation = passivation;
   }

   public Boolean isPassivation() {
      return passivation;
   }

   /**
    * This setting should be set to true when multiple cache instances share the same cache store
    * (e.g., multiple nodes in a cluster using a JDBC-based CacheStore pointing to the same, shared
    * database.) Setting this to true avoids multiple cache instances writing the same modification
    * multiple times. If enabled, only the node where the modification originated will write to the
    * cache store. <br />
    * <br />
    * If disabled, each individual cache reacts to a potential remote update by storing the data to
    * the cache store. Note that this could be useful if each individual node has its own cache
    * store - perhaps local on-disk.
    * 
    * @param shared
    */   
   @Override
   public LoadersConfig shared(Boolean shared) {
      testImmutability("shared");
      this.shared = shared;
      return this;
   }

   /**
    * @deprecated The visibility of this method will be reduced. Use {@link #shared(Boolean)} instead.
    */
   @XmlAttribute
   @Deprecated
   public void setShared(Boolean shared) {
      testImmutability("shared");
      this.shared = shared;
   }

   public Boolean isShared() {
      return shared;
   }

   @Override
   protected CacheLoaderManagerConfig setConfiguration(Configuration config) {
      super.setConfiguration(config);
      return this;
   }

}
