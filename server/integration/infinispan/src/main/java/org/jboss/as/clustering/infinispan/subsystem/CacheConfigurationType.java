/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
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

package org.jboss.as.clustering.infinispan.subsystem;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;

/**
 * Enumerates the supported cache types.
 * @author Paul Ferraro
 */
enum CacheConfigurationType {
    LOCAL(ModelKeys.LOCAL_CACHE_CONFIGURATION, new LocalCacheConfigurationAdd(), new CacheConfigurationRemove()),
    DISTRIBUTED(ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION, new DistributedCacheConfigurationAdd(), LOCAL.getRemoveHandler()),
    REPLICATED(ModelKeys.REPLICATED_CACHE_CONFIGURATION, new ReplicatedCacheConfigurationAdd(), LOCAL.getRemoveHandler()),
    INVALIDATION(ModelKeys.INVALIDATION_CACHE_CONFIGURATION, new InvalidationCacheConfigurationAdd(), LOCAL.getRemoveHandler()),
    ;

    private static final Map<String, CacheConfigurationType> TYPES = new HashMap<>();
    static {
        for (CacheConfigurationType type: values()) {
            TYPES.put(type.key, type);
        }
    }

    static CacheConfigurationType forName(String key) {
        return TYPES.get(key);
    }

    private final String key;
    private final CacheConfigurationAdd addHandler;
    private final CacheConfigurationRemove removeHandler;

    private CacheConfigurationType(String key, CacheConfigurationAdd addHandler, CacheConfigurationRemove removeHandler) {
        this.key = key;
        this.addHandler = addHandler;
        this.removeHandler = removeHandler;
    }

    public ResourceDescriptionResolver getResourceDescriptionResolver() {
        return new InfinispanResourceDescriptionResolver(this.key);
    }

    public PathElement pathElement() {
        return pathElement(PathElement.WILDCARD_VALUE);
    }

    public PathElement pathElement(String name) {
        return PathElement.pathElement(this.key, name);
    }

    public CacheConfigurationAdd getAddHandler() {
        return this.addHandler;
    }

    public CacheConfigurationRemove getRemoveHandler() {
        return this.removeHandler;
    }

    public boolean hasSharedState() {
        return EnumSet.of(REPLICATED, DISTRIBUTED).contains(this);
    }
}
