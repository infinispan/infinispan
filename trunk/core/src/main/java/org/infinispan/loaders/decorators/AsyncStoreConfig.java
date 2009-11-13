package org.infinispan.loaders.decorators;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.Dynamic;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Configuration for the async cache loader
 * 
 * <p>
 * Note that class AsyncStoreConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 *
 * @configRef name="async",parentName="loader",desc="Configuration for the async cache loader.  If enabled, this provides
 *                   you with asynchronous writes to the cache store, giving you 'write-behind' caching."
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class AsyncStoreConfig extends AbstractNamedCacheConfigurationBean {

   /** The serialVersionUID */
   private static final long serialVersionUID = -8596800049019004961L;

   /** @configRef desc="If true, all modifications to this cache store happen asynchronously, on a separate thread." */
   protected Boolean enabled = false;

   /** @configRef desc="Size of the thread pool whose threads are responsible for applying the modifications." */
   protected Integer threadPoolSize = 1;

   /** @configRef desc="Timeout to acquire the lock which guards the state to be flushed to the cache store periodically." */
   @Dynamic
   protected Long flushLockTimeout = 5000L;

   @XmlAttribute
   public Boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(Boolean enabled) {
      testImmutability("enabled");
      this.enabled = enabled;
   }

   @XmlAttribute
   public Integer getThreadPoolSize() {
      return threadPoolSize;
   }

   public void setThreadPoolSize(Integer threadPoolSize) {
      testImmutability("threadPoolSize");
      this.threadPoolSize = threadPoolSize;
   }

   @XmlAttribute
   public Long getFlushLockTimeout() {
      return flushLockTimeout;
   }

   public void setFlushLockTimeout(Long stateLockTimeout) {
      testImmutability("flushLockTimeout");
      this.flushLockTimeout = stateLockTimeout;
   }   

   @Override
   public AsyncStoreConfig clone() {
      try {
         return (AsyncStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
   }

   public void accept(ConfigurationBeanVisitor v) {
      v.visitAsyncStoreConfig(this);
   }
}
