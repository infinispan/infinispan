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

package org.infinispan.spring.provider;

import java.util.Collection;

import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.CacheManager <code>CacheManager</code>} implementation that is
 * backed by an {@link org.infinispan.manager.EmbeddedCacheManager
 * <code>Infinispan EmbeddedCacheManager</code>} instance.
 * </p>
 * <p>
 * Note that this <code>CacheManager</code> <strong>does</strong> support adding new
 * {@link org.infinispan.Cache <code>Caches</code>} at runtime, i.e. <code>Caches</code> added
 * programmatically to the backing <code>EmbeddedCacheManager</code> after this
 * <code>CacheManager</code> has been constructed will be seen by this <code>CacheManager</code>.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
public class SpringEmbeddedCacheManager implements CacheManager {

   private final EmbeddedCacheManager nativeCacheManager;

   /**
    * @param nativeCacheManager
    */
   public SpringEmbeddedCacheManager(final EmbeddedCacheManager nativeCacheManager) {
      Assert.notNull(nativeCacheManager,
               "A non-null instance of EmbeddedCacheManager needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
   }

   @Override
   public SpringCache getCache(final String name) {
      return new SpringCache(this.nativeCacheManager.getCache(name));
   }

   @Override
   public Collection<String> getCacheNames() {
      return this.nativeCacheManager.getCacheNames();
   }

   /**
    * Return the {@link org.infinispan.manager.EmbeddedCacheManager
    * <code>org.infinispan.manager.EmbeddedCacheManager</code>} that backs this
    * <code>CacheManager</code>.
    * 
    * @return The {@link org.infinispan.manager.EmbeddedCacheManager
    *         <code>org.infinispan.manager.EmbeddedCacheManager</code>} that backs this
    *         <code>CacheManager</code>
    */
   public EmbeddedCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   /**
    * Stop the {@link EmbeddedCacheManager <code>EmbeddedCacheManager</code>} this
    * <code>CacheManager</code> delegates to.
    */
   public void stop() {
      this.nativeCacheManager.stop();
   }
}
