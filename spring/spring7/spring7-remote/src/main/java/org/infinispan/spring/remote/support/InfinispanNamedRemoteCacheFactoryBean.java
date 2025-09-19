package org.infinispan.spring.remote.support;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * <p>
 * A {@link FactoryBean <code>FactoryBean</code>} for creating a
 * native {@link #setCacheName(String) named} Infinispan {@link org.infinispan.Cache
 * <code>org.infinispan.Cache</code>}, delegating to a
 * {@link #setInfinispanRemoteCacheManager(RemoteCacheManager) <code>configurable</code>}
 * {@link RemoteCacheManager
 * <code>oorg.infinispan.client.hotrod.RemoteCacheManagerr</code>}. If no cache name is explicitly
 * set, this <code>FactoryBean</code>'s {@link #setBeanName(String) <code>beanName</code>} will be
 * used instead.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
public class InfinispanNamedRemoteCacheFactoryBean<K, V> implements FactoryBean<RemoteCache<K, V>>,
                                                                    BeanNameAware, InitializingBean {

   private static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private RemoteCacheManager infinispanRemoteCacheManager;

   private String cacheName;

   private String beanName;

   private RemoteCache<K, V> infinispanCache;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      if (this.infinispanRemoteCacheManager == null) {
         throw new IllegalStateException("No Infinispan RemoteCacheManager has been set");
      }
      logger.info("Initializing named Infinispan remote cache ...");
      final String effectiveCacheName = obtainEffectiveCacheName();
      this.infinispanCache = this.infinispanRemoteCacheManager.getCache(effectiveCacheName);
      logger.info("New Infinispan remote cache [" + this.infinispanCache + "] initialized");
   }

   private String obtainEffectiveCacheName() {
      if (StringUtils.hasText(this.cacheName)) {
         if (logger.isDebugEnabled()) {
            logger.debug("Using custom cache name [" + this.cacheName + "]");
         }
         return this.cacheName;
      } else {
         if (logger.isDebugEnabled()) {
            logger.debug("Using bean name [" + this.beanName + "] as cache name");
         }
         return this.beanName;
      }
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.BeanNameAware
   // ------------------------------------------------------------------------

   /**
    * @see BeanNameAware#setBeanName(String)
    */
   @Override
   public void setBeanName(final String name) {
      this.beanName = name;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see FactoryBean#getObject()
    */
   @Override
   public RemoteCache<K, V> getObject() {
      return this.infinispanCache;
   }

   /**
    * @see FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends ConcurrentMap> getObjectType() {
      return this.infinispanCache != null ? this.infinispanCache.getClass() : RemoteCache.class;
   }

   /**
    * Always return true.
    *
    * @see FactoryBean#isSingleton()
    */
   @Override
   public boolean isSingleton() {
      return true;
   }

   // ------------------------------------------------------------------------
   // Properties
   // ------------------------------------------------------------------------

   /**
    * <p>
    * Sets the {@link org.infinispan.Cache#getName() name} of the {@link org.infinispan.Cache
    * <code>org.infinispan.Cache</code>} to be created. If no explicit <code>cacheName</code> is
    * set, this <code>FactoryBean</code> will use its {@link #setBeanName(String)
    * <code>beanName</code>} as the <code>cacheName</code>.
    * </p>
    *
    * @param cacheName
    *           The {@link org.infinispan.Cache#getName() name} of the {@link org.infinispan.Cache
    *           <code>org.infinispan.Cache</code>} to be created
    */
   public void setCacheName(final String cacheName) {
      this.cacheName = cacheName;
   }

   /**
    * <p>
    * Sets the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} to be used for creating our
    * {@link org.infinispan.Cache <code>Cache</code>} instance. Note that this is a
    * <b>mandatory</b> property.
    * </p>
    *
    * @param infinispanRemoteCacheManager
    *           The {@link RemoteCacheManager
    *           <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} to be used for
    *           creating our {@link org.infinispan.Cache <code>Cache</code>} instance
    */
   public void setInfinispanRemoteCacheManager(final RemoteCacheManager infinispanRemoteCacheManager) {
      this.infinispanRemoteCacheManager = infinispanRemoteCacheManager;
   }
}
