/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;

/**
 * CacheContainerConfigurationsResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class CacheContainerConfigurationsResource extends SimpleResourceDefinition {
    static final PathElement PATH = PathElement.pathElement(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME);
    private final boolean runtimeRegistration;
    private final ResolvePathHandler resolvePathHandler;

    CacheContainerConfigurationsResource(ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(PATH,
              new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER, ModelKeys.CONFIGURATIONS),
              new ReloadRequiredAddStepHandler(),
              ReloadRequiredRemoveStepHandler.INSTANCE);

        this.resolvePathHandler = resolvePathHandler;
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        resourceRegistration.registerSubModel(new LocalCacheConfigurationResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new InvalidationCacheConfigurationResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new ReplicatedCacheConfigurationResource(resolvePathHandler, runtimeRegistration));
        resourceRegistration.registerSubModel(new DistributedCacheConfigurationResource(resolvePathHandler, runtimeRegistration));
    }
}
