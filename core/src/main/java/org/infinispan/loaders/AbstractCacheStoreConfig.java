package org.infinispan.loaders;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Configures {@link AbstractCacheStore}.  This allows you to tune a number of characteristics of the {@link
 * AbstractCacheStore}.
 * <p/>
 * <ul> <li><tt>purgeSynchronously</tt> - whether {@link org.infinispan.loaders.CacheStore#purgeExpired()} calls happen
 * synchronously or not.  By default, this is set to <tt>false</tt>.</li>
 * <p/>
 * </ul>
 * 
 * <p>
 * Note that class AbstractCacheStoreConfig contains JAXB annotations. These annotations determine how XML
 * configuration files are read into instances of configuration class hierarchy as well as they
 * provide meta data for configuration file XML schema generation. Please modify these annotations
 * and Java element types they annotate with utmost understanding and care.
 * 
 * @configRef name="loader",desc="Responsible for loading/storing cache data from/to an external source." 
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder={"singletonStore","async"})
public class AbstractCacheStoreConfig extends AbstractCacheLoaderConfig implements CacheStoreConfig {
   
   /** 
    * @configRef desc="If true, any operation that modifies the cache store (remove, clear, store...etc) won't be applied to it"
    * */
   @XmlAttribute
   protected Boolean ignoreModifications = false;
   
   /**
    *  @configRef desc="If true, fetch persistent state on state transfer"
    *  */
   @XmlAttribute
   protected Boolean fetchPersistentState = false;
   
   /**
    *  @configRef desc="If true, purge node state on startup"
    *  */
   @XmlAttribute
   protected Boolean purgeOnStartup = false;
   
   /**
    *  @configRef desc="If true, CacheStore#purgeExpired() call will be done synchronously"
    *  */
   @XmlAttribute
   protected Boolean purgeSynchronously = false;
   
   @XmlElement
   protected SingletonStoreConfig singletonStore = new SingletonStoreConfig();
   
   @XmlElement
   protected AsyncStoreConfig async = new AsyncStoreConfig();

   

   public boolean isPurgeSynchronously() {
      return purgeSynchronously;
   }

   public void setPurgeSynchronously(boolean purgeSynchronously) {
      testImmutability("purgeSynchronously");
      this.purgeSynchronously = purgeSynchronously;
   }

   public boolean isPurgeOnStartup() {
      return purgeOnStartup;
   }

   public boolean isFetchPersistentState() {
      return fetchPersistentState;
   }

   public void setFetchPersistentState(boolean fetchPersistentState) {
      testImmutability("fetchPersistentState");
      this.fetchPersistentState = fetchPersistentState;
   }

   public void setIgnoreModifications(boolean ignoreModifications) {
      testImmutability("ignoreModifications");
      this.ignoreModifications = ignoreModifications;
   }

   public boolean isIgnoreModifications() {
      return ignoreModifications;
   }

   public void setPurgeOnStartup(boolean purgeOnStartup) {
      testImmutability("purgeOnStartup");
      this.purgeOnStartup = purgeOnStartup;
   }

   public SingletonStoreConfig getSingletonStoreConfig() {
      return singletonStore;
   }

   public void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig) {
      testImmutability("singletonStore");
      this.singletonStore = singletonStoreConfig;
   }

   public AsyncStoreConfig getAsyncStoreConfig() {
      return async;
   }

   public void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig) {
      testImmutability("async");
      this.async = asyncStoreConfig;
   }
   
   public void accept(ConfigurationBeanVisitor v) {
      singletonStore.accept(v);
      async.accept(v);
      v.visitCacheLoaderConfig(this);
   }

   @Override
   public boolean equals(Object obj) {
      if (super.equals(obj)) {
         if (!(obj instanceof AbstractCacheStoreConfig)) return false;
         AbstractCacheStoreConfig i = (AbstractCacheStoreConfig) obj;
         return equalsExcludingProperties(i);
      }
      return false;
   }

   protected boolean equalsExcludingProperties(Object obj) {
      AbstractCacheStoreConfig other = (AbstractCacheStoreConfig) obj;

      return Util.safeEquals(this.cacheLoaderClassName, other.cacheLoaderClassName)
            && (this.ignoreModifications.equals(other.ignoreModifications))
            && (this.fetchPersistentState.equals(other.fetchPersistentState))
            && Util.safeEquals(this.singletonStore, other.singletonStore)
            && Util.safeEquals(this.async, other.async)
            && Util.safeEquals(this.purgeSynchronously, other.purgeSynchronously);
   }

   @Override
   public int hashCode() {
      return 31 * hashCodeExcludingProperties() + (properties == null ? 0 : properties.hashCode());
   }

   protected int hashCodeExcludingProperties() {
      int result = 17;
      result = 31 * result + (cacheLoaderClassName == null ? 0 : cacheLoaderClassName.hashCode());
      result = 31 * result + (ignoreModifications ? 0 : 1);
      result = 31 * result + (fetchPersistentState ? 0 : 1);
      result = 31 * result + (singletonStore == null ? 0 : singletonStore.hashCode());
      result = 31 * result + (async == null ? 0 : async.hashCode());
      result = 31 * result + (purgeOnStartup ? 0 : 1);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder().append(getClass().getSimpleName()).append("{").append("className='").append(cacheLoaderClassName).append('\'')
            .append(", ignoreModifications=").append(ignoreModifications)
            .append(", fetchPersistentState=").append(fetchPersistentState)
            .append(", properties=").append(properties)
            .append(", purgeOnStartup=").append(purgeOnStartup).append("},")
            .append(", singletonStore{").append(singletonStore).append('}')
            .append(", async{").append(async).append('}')
            .append(", purgeSynchronously{").append(purgeSynchronously).append('}')
            .toString();
   }

   @Override
   public AbstractCacheStoreConfig clone() {
      AbstractCacheStoreConfig clone = (AbstractCacheStoreConfig) super.clone();
      if (singletonStore != null) clone.setSingletonStoreConfig(singletonStore.clone());
      if (async != null) clone.setAsyncStoreConfig(async.clone());
      return clone;
   }
}
