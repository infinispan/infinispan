package org.infinispan.loaders;

import org.infinispan.config.ConfigurationBeanVisitor;
import org.infinispan.config.ConfigurationDoc;
import org.infinispan.config.ConfigurationDocRef;
import org.infinispan.loaders.decorators.AsyncStoreConfig;
import org.infinispan.loaders.decorators.SingletonStoreConfig;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.*;

/**
 * Configures {@link AbstractCacheStore}. This allows you to tune a number of characteristics of the
 * {@link AbstractCacheStore}.
 * <p/>
 * <ul>
 * <li><tt>purgeSynchronously</tt> - whether
 * {@link org.infinispan.loaders.CacheStore#purgeExpired()} calls happen synchronously or not. By
 * default, this is set to <tt>false</tt>.</li>
 * <li><tt>purgerThreads</tt> - number of threads to use when purging. Defaults to <tt>1</tt> if
 * <tt>purgeSynchronously</tt> is <tt>true</tt>, ignored if <tt>false</tt>.</li>
 * </ul>
 * 
 * 
 * 
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @since 4.0
 * 
 * @see <a href="../../../config.html#ce_loaders_loader">Configuration reference</a>
 */
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
@XmlType(propOrder= {})
@ConfigurationDoc(name="loader",desc="Responsible for loading/storing cache data from/to an external source.")
@SuppressWarnings("boxing")
public class AbstractCacheStoreConfig extends AbstractCacheLoaderConfig implements CacheStoreConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = 4607371052771122893L;
   
   @ConfigurationDocRef(bean=AbstractCacheStoreConfig.class,targetElement="setIgnoreModifications")
   protected Boolean ignoreModifications = false;

   @ConfigurationDocRef(bean=AbstractCacheStoreConfig.class,targetElement="setFetchPersistentState")
   protected Boolean fetchPersistentState = false;

   @ConfigurationDocRef(bean=AbstractCacheStoreConfig.class,targetElement="setPurgeOnStartup")
   protected Boolean purgeOnStartup = false;

   @ConfigurationDocRef(bean=AbstractCacheStoreConfig.class,targetElement="setPurgeSynchronously")
   protected Boolean purgeSynchronously = false;

   @ConfigurationDocRef(bean=AbstractCacheStoreConfig.class,targetElement="setPurgerThreads")
   protected Integer purgerThreads = 1;

   protected SingletonStoreConfig singletonStore = new SingletonStoreConfig();

   protected AsyncStoreConfig async = new AsyncStoreConfig();
   

   @Override
   public AsyncStoreConfig asyncStore() {
      async.setEnabled(true);
      async.setCacheStoreConfig(this);
      return async;
   }

   @Override
   public SingletonStoreConfig singletonStore() {
      singletonStore.setSingletonStoreEnabled(true);
      singletonStore.setCacheStoreConfig(this);
      return singletonStore;
   }

   @Override
   @XmlAttribute
   public Boolean isPurgeSynchronously() {
      return purgeSynchronously;
   }

   @Override
   @XmlAttribute
   public Integer getPurgerThreads() {
      return purgerThreads;
   }
   
   @XmlElement(name="properties")
   public TypedProperties getTypedProperties(){
      return properties;      
   }
   
   public void setTypedProperties (TypedProperties tp){
      this.properties = tp;
   }

   /**
    * If true, CacheStore#purgeExpired() call will be done synchronously
    */
   @Override
   public void setPurgeSynchronously(Boolean purgeSynchronously) {
      testImmutability("purgeSynchronously");
      this.purgeSynchronously = purgeSynchronously;
   }
   
   /**
    * If true, CacheStore#purgeExpired() call will be done synchronously
    * 
    * @param purgeSynchronously
    */
   @Override
   public CacheStoreConfig purgeSynchronously(Boolean purgeSynchronously) {
      testImmutability("purgeSynchronously");
      this.purgeSynchronously = purgeSynchronously;
      return this;
   }

   /**
    * The number of threads to use when purging asynchronously.
    * 
    * @param purgerThreads
    * @deprecated use {@link #purgerThreads(Integer)} instead
   */
   @Deprecated
   public void setPurgerThreads(Integer purgerThreads) {
      testImmutability("purgerThreads");
      this.purgerThreads = purgerThreads;
   }

   @Override
   public CacheStoreConfig purgerThreads(Integer purgerThreads) {
      setPurgerThreads(purgerThreads);
      return this;
   }

   @Override
   @XmlAttribute
   public Boolean isFetchPersistentState() {
      return fetchPersistentState;
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    * 
    * 
    * @param fetchPersistentState
    */
   @Override
   public void setFetchPersistentState(Boolean fetchPersistentState) {
      testImmutability("fetchPersistentState");
      this.fetchPersistentState = fetchPersistentState;
   }
   
   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    * 
    * 
    * @param fetchPersistentState
    */
   @Override
   public CacheStoreConfig fetchPersistentState(Boolean fetchPersistentState) {
      testImmutability("fetchPersistentState");
      this.fetchPersistentState = fetchPersistentState;
      return this;
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    * 
    * @param ignoreModifications
    */
   @Override
   public void setIgnoreModifications(Boolean ignoreModifications) {
      testImmutability("ignoreModifications");
      this.ignoreModifications = ignoreModifications;    
   }
   
   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    * 
    * @param ignoreModifications
    */
   @Override
   public CacheStoreConfig ignoreModifications(Boolean ignoreModifications) {
      testImmutability("ignoreModifications");
      this.ignoreModifications = ignoreModifications;
      return this;
   }

   @Override
   @XmlAttribute
   public Boolean isIgnoreModifications() {
      return ignoreModifications;
   }
   
   @Override
   @XmlAttribute
   public Boolean isPurgeOnStartup() {
      return purgeOnStartup;
   }

   /**
    * 
    * If true, purges this cache store when it starts up.
    * 
    * @param purgeOnStartup
    */
   @Override
   public CacheStoreConfig purgeOnStartup(Boolean purgeOnStartup) {
      testImmutability("purgeOnStartup");
      this.purgeOnStartup = purgeOnStartup;
      return this;
   }
   
   /**
    * 
    * If true, purges this cache store when it starts up.
    * 
    * @param purgeOnStartup
    */
   @Override
   public void setPurgeOnStartup(Boolean purgeOnStartup) {
      testImmutability("purgeOnStartup");
      this.purgeOnStartup = purgeOnStartup;
   }

   @Override
   @XmlElement(name="singletonStore")
   public SingletonStoreConfig getSingletonStoreConfig() {
      return singletonStore;
   }

   @Override
   public void setSingletonStoreConfig(SingletonStoreConfig singletonStoreConfig) {
      testImmutability("singletonStore");
      this.singletonStore = singletonStoreConfig;
   }

   @Override
   @XmlElement(name="async")
   public AsyncStoreConfig getAsyncStoreConfig() {
      return async;      
   }

   @Override
   public void setAsyncStoreConfig(AsyncStoreConfig asyncStoreConfig) {
      testImmutability("async");
      this.async = asyncStoreConfig;      
   }
   
   @Override
   public void accept(ConfigurationBeanVisitor v) {
      singletonStore.accept(v);
      async.accept(v);
//      v.visitCacheLoaderConfig(this);
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
            && Util.safeEquals(this.purgeSynchronously, other.purgeSynchronously)
            && Util.safeEquals(this.purgerThreads, other.purgerThreads);
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
      result = 31 * result + (purgerThreads);
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
            .append(", purgerThreads{").append(purgerThreads).append('}')
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
