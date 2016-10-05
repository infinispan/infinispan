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

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.springframework.cache.Cache;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.CacheManager <code>CacheManager</code>} implementation that is
 * backed by an {@link org.infinispan.client.hotrod.RemoteCacheManager
 * <code>Infinispan RemoteCacheManager</code>} instance.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
public class SpringRemoteCacheManager implements org.springframework.cache.CacheManager {

   private final RemoteCacheManager nativeCacheManager;
   private boolean useAsynchronousCacheOperations = false;

   /**
    * @param nativeCacheManager
    */
   public SpringRemoteCacheManager(final RemoteCacheManager nativeCacheManager) {
      Assert.notNull(nativeCacheManager,
               "A non-null instance of EmbeddedCacheManager needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
   }

   /**
    * @see org.springframework.cache.CacheManager#getCache(java.lang.String)
    */
   @Override
   public Cache getCache(final String name) {
       if (this.useAsynchronousCacheOperations) {
           return new SpringAsynchronousCache(this.nativeCacheManager.getCache(name));
       } else {
           return new SpringCache(this.nativeCacheManager.getCache(name));
       }
   }

   /**
    * <p>
    * As of Infinispan 4.2.0.FINAL <code>org.infinispan.client.hotrod.RemoteCache</code> does
    * <strong>not</strong> support retrieving the set of all cache names from the hotrod server.
    * This restriction may be lifted in the future. Currently, this operation will always throw an
    * <code>UnsupportedOperationException</code>.
    * </p>
    * 
    * @see org.springframework.cache.CacheManager#getCacheNames()
    */
   @Override
   public Collection<String> getCacheNames() {
      throw new UnsupportedOperationException(
               "Operation getCacheNames() is currently not supported.");
   }

   /**
    * Return the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    * 
    * @return The {@link org.infinispan.client.hotrod.RemoteCacheManager
    *         <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    *         <code>SpringRemoteCacheManager</code>
    */
   public RemoteCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   /**
    * Start the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void start() {
      this.nativeCacheManager.start();
   }

   /**
    * Stop the {@link org.infinispan.client.hotrod.RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void stop() {
      this.nativeCacheManager.stop();
   }
    public boolean isUseAsynchronousCacheOperations() {
       return useAsynchronousCacheOperations;
    }

    /**
     * Set a value indicating if the SpringCache's returned by this CacheManager should use
     * using a {@link SpringAsynchronousCache} rather than a {@link SpringCache}
     *
     * Setting this value only affects any future calls to {@link #getCache(String)}.
     *
     * The default value is false.
     *
     * @param useAsynchronousCacheOperations
     */
    public void setUseAsynchronousCacheOperations(boolean useAsynchronousCacheOperations) {
       this.useAsynchronousCacheOperations = useAsynchronousCacheOperations;
    }
}
