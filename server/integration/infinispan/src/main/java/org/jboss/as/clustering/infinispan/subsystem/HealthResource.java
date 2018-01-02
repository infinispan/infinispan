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
import org.jboss.as.controller.services.path.PathManager;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/health=HEALTH
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class HealthResource extends SimpleResourceDefinition {

    public static final PathElement HEALTH_PATH = PathElement.pathElement(ModelKeys.HEALTH, ModelKeys.HEALTH_NAME);

    private final PathManager pathManager;
    private final boolean isRuntimeRegistration;

    public HealthResource(PathManager pathManager, boolean isRuntimeRegistration) {
        super(HEALTH_PATH,
                new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER, ModelKeys.HEALTH),
                new ReloadRequiredAddStepHandler(),
                ReloadRequiredRemoveStepHandler.INSTANCE);
        this.pathManager = pathManager;
        this.isRuntimeRegistration = isRuntimeRegistration;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        if(isRuntimeRegistration) {
            HealthMetricsHandler.INSTANCE.registerPathManager(pathManager);
            HealthMetricsHandler.INSTANCE.registerMetrics(resourceRegistration);
        }
    }

    @Override
    public boolean isRuntime() {
        return true;
    }
}
