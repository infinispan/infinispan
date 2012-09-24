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

package org.infinispan.spring.support.embedded;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.AbstractEmbeddedCacheManagerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * <p>
 * A {@link org.springframework.beans.factory.FactoryBean <code>FactoryBean</code>} for creating an
 * {@link org.infinispan.manager.EmbeddedCacheManager <code>Infinispan EmbeddedCacheManager</code>}
 * instance. The location of the Infinispan configuration file used to provide the default
 * {@link org.infinispan.configuration.cache.Configuration configuration} for the
 * <code>EmbeddedCacheManager</code> instance created by this <code>FactoryBean</code> is
 * {@link #setConfigurationFileLocation(org.springframework.core.io.Resource) configurable}.
 * </p>
 * <p>
 * If no configuration file location is set the <code>EmbeddedCacheManager</code> instance created
 * by this <code>FactoryBean</code> will use Infinispan's default settings. See Infinispan's <a
 * href="http://www.jboss.org/infinispan/docs">documentation</a> for what those default settings
 * are.
 * </p>
 * <p>
 * A user may further customize the <code>EmbeddedCacheManager</code>'s configuration using explicit
 * setters on this <code>FactoryBean</code>. The properties thus defined will be applied either to
 * the configuration loaded from Infinispan's configuration file in case one has been specified, or
 * to a configuration initialized with Infinispan's default settings. Either way, the net effect is
 * that explicitly set configuration properties take precedence over both those loaded from a
 * configuration file as well as INFNISPAN's default settings.
 * </p>
 * <p>
 * In addition to creating an <code>EmbeddedCacheManager</code> this <code>FactoryBean</code> does
 * also control that <code>EmbeddedCacheManagers</code>'s {@link org.infinispan.lifecycle.Lifecycle
 * lifecycle} by shutting it down when the enclosing Spring application context is closed. It is
 * therefore advisable to <em>always</em> use this <code>FactoryBean</code> when creating an
 * <code>EmbeddedCacheManager</code>.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 * @see #setConfigurationFileLocation(org.springframework.core.io.Resource)
 * @see #destroy()
 * @see org.infinispan.manager.EmbeddedCacheManager
 * @see org.infinispan.configuration.cache.Configuration
 * 
 */
public class InfinispanEmbeddedCacheManagerFactoryBean extends AbstractEmbeddedCacheManagerFactory
         implements FactoryBean<EmbeddedCacheManager>, InitializingBean, DisposableBean {

   private EmbeddedCacheManager cacheManager;

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.InitializingBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
    */
   @Override
   public void afterPropertiesSet() throws Exception {
      logger.info("Initializing Infinispan EmbeddedCacheManager instance ...");

      this.cacheManager = createBackingEmbeddedCacheManager();

      logger.info("Successfully initialized Infinispan EmbeddedCacheManager instance ["
               + this.cacheManager + "]");
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.FactoryBean
   // ------------------------------------------------------------------------

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObject()
    */
   @Override
   public EmbeddedCacheManager getObject() throws Exception {
      return this.cacheManager;
   }

   /**
    * @see org.springframework.beans.factory.FactoryBean#getObjectType()
    */
   @Override
   public Class<? extends EmbeddedCacheManager> getObjectType() {
      return this.cacheManager != null ? this.cacheManager.getClass() : EmbeddedCacheManager.class;
   }

   /**
    * Always returns <code>true</code>.
    * 
    * @return Always <code>true</code>
    * 
    * @see org.springframework.beans.factory.FactoryBean#isSingleton()
    */
   @Override
   public boolean isSingleton() {
      return true;
   }

   // ------------------------------------------------------------------------
   // org.springframework.beans.factory.DisposableBean
   // ------------------------------------------------------------------------

   /**
    * Shuts down the <code>EmbeddedCacheManager</code> instance created by this
    * <code>FactoryBean</code>.
    * 
    * @see org.springframework.beans.factory.DisposableBean#destroy()
    * @see org.infinispan.manager.EmbeddedCacheManager#stop()
    */
   @Override
   public void destroy() throws Exception {
      // Probably being paranoid here ...
      if (this.cacheManager != null) {
         this.cacheManager.stop();
      }
   }
}
