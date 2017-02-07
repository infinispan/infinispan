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

import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Base class for cache resources which require common cache attributes and clustered cache attributes.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public class ClusteredCacheConfigurationResource extends CacheConfigurationResource {
    // the attribute definition for the cache mode
    static final SimpleAttributeDefinition MODE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.MODE, ModelType.STRING, true)
                    .setXmlName(Attribute.MODE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(Mode.class, false, true))
                    .setAlternatives(ModelKeys.CONFIGURATION)
                    .setDefaultValue(new ModelNode().set(Mode.SYNC.toString()))
                    .build();

    static final SimpleAttributeDefinition REMOTE_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.REMOTE_TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.REMOTE_TIMEOUT.getLocalName())
                    .setMeasurementUnit(MeasurementUnit.MILLISECONDS)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(ClusteringConfiguration.REMOTE_TIMEOUT.getDefaultValue()))
                    .build();

    static final AttributeDefinition[] ATTRIBUTES = { MODE, REMOTE_TIMEOUT };

    public ClusteredCacheConfigurationResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver, CacheConfigurationAdd addHandler, OperationStepHandler removeHandler, ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(pathElement, descriptionResolver, addHandler, removeHandler, resolvePathHandler, runtimeRegistration);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        final OperationStepHandler restartWriteHandler = new RestartCacheWriteAttributeHandler(getPathElement().getKey(), getServiceInstaller(), ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartWriteHandler);
        }
    }
}
