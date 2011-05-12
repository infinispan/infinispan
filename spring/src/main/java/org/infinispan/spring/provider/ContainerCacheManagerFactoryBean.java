/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *    ~
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

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.provider.SpringEmbeddedCacheManager;
import org.infinispan.spring.provider.SpringRemoteCacheManager;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;

/**
 * {@link FactoryBean} for creating a {@link CacheManager} for a pre-defined {@link org.infinispan.manager.CacheContainer}.
 * <p/>
 * Useful when the cache container is defined outside the application (e.g. provided by the application server)
 *
 * @author Marius Bogoevici
 */
public class ContainerCacheManagerFactoryBean implements FactoryBean<CacheManager> {

    private CacheContainer cacheContainer;

    public ContainerCacheManagerFactoryBean(CacheContainer cacheContainer) {
        Assert.notNull(cacheContainer, "CacheContainer cannot be null");
        if (!(cacheContainer instanceof EmbeddedCacheManager ||
                cacheContainer instanceof RemoteCacheManager)) {
            throw new IllegalArgumentException("CacheContainer must be either an EmbeddedCacheManager or a RemoteCacheManager ");
        }
        this.cacheContainer = cacheContainer;
    }

    @Override
    public CacheManager getObject() throws Exception {
        if (this.cacheContainer instanceof EmbeddedCacheManager) {
            return new SpringEmbeddedCacheManager((EmbeddedCacheManager) this.cacheContainer);
        } else if (this.cacheContainer instanceof RemoteCacheManager) {
            return new SpringRemoteCacheManager((RemoteCacheManager) this.cacheContainer);
        } else {
            throw new IllegalArgumentException("CacheContainer must be either an EmbeddedCacheManager or a RemoteCacheManager ");
        }
    }

    @Override
    public Class<?> getObjectType() {
        return CacheManager.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
