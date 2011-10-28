/**
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *   ~
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.spring.support.remote;

import org.infinispan.BasicCache;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

/**
 * <p>
 * A {@link org.springframework.beans.factory.FactoryBean <code>FactoryBean</code>} for creating a
 * native {@link #setCacheName(String) named} Infinispan {@link org.infinispan.Cache
 * <code>org.infinispan.Cache</code>}, delegating to a
 * {@link #setInfinispanRemoteCacheManager(RemoteCacheManager) <code>configurable</code>}
 * {@link org.infinispan.client.hotrod.RemoteCacheManager
 * <code>oorg.infinispan.client.hotrod.RemoteCacheManagerr</code>}. If no cache name is explicitly
 * set, this <code>FactoryBean</code>'s {@link #setBeanName(String) <code>beanName</code>} will be
 * used instead.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
public class InfinispanNamedRemoteCacheFactoryBean<K, V> implements FactoryBean<BasicCache<K, V>>,
         BeanNameAware, InitializingBean {

   private final Log logger = LogFactory.getLog(getClass());

   private RemoteCacheManager infinispanRemoteCacheManager;

   private String cacheName;

   private String beanName;

   private BasicCache<K, V> infinispanCache;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      if (this.infinispanRemoteCacheManager == null) {
         throw new IllegalStateException("No Infinispan RemoteCacheManager has been set");
      }
      this.logger.info("Initializing named Infinispan remote cache ...");
      final String effectiveCacheName = obtainEffectiveCacheName();
      this.infinispanCache = this.infinispanRemoteCacheManager.getCache(effectiveCacheName);
      this.logger.info("New Infinispan remote cache [" + this.infinispanCache + "] initialized");
   }

   private String obtainEffectiveCacheName() {
      if (StringUtils.hasText(this.cacheName)) {
         if (this.logger.isDebugEnabled()) {
            this.logger.debug("Using custom cache name [" + this.cacheName + "]");
         }
         return this.cacheName;
      } else {
         if (this.logger.isDebugEnabled()) {
            this.logger.debug("Using bean name [" + this.beanName + "] as cache name");
         }
         return this.beanName;
      }
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.BeanNameAware
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.BeanNameAware#setBeanName(java.lang.String)
    */
   @Override
   public void setBeanName(final String name) {
      this.beanName = name;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObject()
    */
   @Override
   public BasicCache<K, V> getObject() throws Exception {
      return this.infinispanCache;
   }

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends BasicCache> getObjectType() {
      return this.infinispanCache != null ? this.infinispanCache.getClass() : Cache.class;
   }

   /**
    * Always return true.
    * 
    * @see org.springframework.beans.factory.FactoryBean#isSingleton()
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
    * Sets the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} to be used for creating our
    * {@link org.infinispan.Cache <code>Cache</code>} instance. Note that this is a
    * <strong>mandatory</strong> property.
    * </p>
    * 
    * @param infinispanRemoteCacheManager
    *           The {@link org.infinispan.client.hotrod.RemoteCacheManager
    *           <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} to be used for
    *           creating our {@link org.infinispan.Cache <code>Cache</code>} instance
    */
   public void setInfinispanRemoteCacheManager(final RemoteCacheManager infinispanRemoteCacheManager) {
      this.infinispanRemoteCacheManager = infinispanRemoteCacheManager;
   }
}
