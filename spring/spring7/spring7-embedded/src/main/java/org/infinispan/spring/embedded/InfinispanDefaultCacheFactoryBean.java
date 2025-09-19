package org.infinispan.spring.embedded;

import java.lang.invoke.MethodHandles;

import org.infinispan.Cache;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.CacheContainer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;



/**
 * <p>
 * A {@link FactoryBean <code>FactoryBean</code>} for creating a
 * native <em>default</em> Infinispan {@link Cache <code>org.infinispan.Cache</code>}
 * , delegating to a {@link #setInfinispanCacheContainer(CacheContainer) <code>configurable</code>}
 * {@link CacheContainer <code>org.infinispan.manager.CacheContainer</code>}.
 * A default <code>Cache</code> is a <code>Cache</code> that uses its <code>CacheContainer</code>'s
 * default settings. This is contrary to a <em>named</em> <code>Cache</code> where select settings
 * from a <code>CacheContainer</code>'s default configuration may be overridden with settings
 * specific to that <code>Cache</code>.
 * </p>
 * <p>
 * In addition to creating a <code>Cache</code> this <code>FactoryBean</code> does also control that
 * <code>Cache</code>'s {@link org.infinispan.commons.api.Lifecycle lifecycle} by shutting it down
 * when the enclosing Spring application context is closed. It is therefore advisable to
 * <em>always</em> use this <code>FactoryBean</code> when creating a <code>Cache</code>.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
public class InfinispanDefaultCacheFactoryBean<K, V> implements FactoryBean<Cache<K, V>>,
      InitializingBean, DisposableBean {

   protected static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private CacheContainer infinispanCacheContainer;

   private Cache<K, V> infinispanCache;

   /**
    * <p>
    * Sets the {@link CacheContainer
    * <code>org.infinispan.manager.CacheContainer</code>} to be used for creating our
    * {@link Cache <code>Cache</code>} instance. Note that this is a
    * <b>mandatory</b> property.
    * </p>
    *
    * @param infinispanCacheContainer
    *           The {@link CacheContainer
    *           <code>org.infinispan.manager.CacheContainer</code>} to be used for creating our
    *           {@link Cache <code>Cache</code>} instance
    */
   public void setInfinispanCacheContainer(final CacheContainer infinispanCacheContainer) {
      this.infinispanCacheContainer = infinispanCacheContainer;
   }

   /**
    * @see InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      if (this.infinispanCacheContainer == null) {
         throw new IllegalStateException("No Infinispan CacheContainer has been set");
      }
      logger.info("Initializing named Infinispan cache ...");
      this.infinispanCache = this.infinispanCacheContainer.getCache();
      logger.info("New Infinispan cache [" + this.infinispanCache + "] initialized");
   }

   /**
    * @see FactoryBean#getObject()
    */
   @Override
   public Cache<K, V> getObject() throws Exception {
      return this.infinispanCache;
   }

   /**
    * @see FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends Cache> getObjectType() {
      return this.infinispanCache != null ? this.infinispanCache.getClass() : Cache.class;
   }

   /**
    * Always returns <code>true</code>.
    *
    * @return Always <code>true</code>
    *
    * @see FactoryBean#isSingleton()
    */
   @Override
   public boolean isSingleton() {
      return true;
   }

   /**
    * Shuts down the <code>org.infinispan.Cache</code> created by this <code>FactoryBean</code>.
    *
    * @see DisposableBean#destroy()
    * @see Cache#stop()
    */
   @Override
   public void destroy() throws Exception {
      // Probably being paranoid here ...
      if (this.infinispanCache != null) {
         this.infinispanCache.stop();
      }
   }
}
