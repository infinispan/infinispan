/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015, Red Hat, Inc., and individual contributors
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.server.commons.controller.AddStepHandler;
import org.infinispan.server.commons.controller.RemoveStepHandler;
import org.infinispan.server.commons.controller.ResourceDescriptor;
import org.infinispan.server.commons.controller.ResourceServiceHandler;
import org.infinispan.server.commons.controller.SimpleResourceServiceHandler;
import org.infinispan.server.commons.controller.validation.IntRangeValidatorBuilder;
import org.infinispan.server.commons.controller.validation.LongRangeValidatorBuilder;
import org.infinispan.server.commons.controller.validation.ParameterValidatorBuilder;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.ResourceDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.access.management.AccessConstraintDefinition;
import org.jboss.as.controller.client.helpers.MeasurementUnit;
import org.jboss.as.controller.descriptions.DefaultResourceDescriptionProvider;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ImmutableManagementResourceRegistration;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceName;

/**
 * Thread pool resource definitions for Infinispan subsystem. See {@link org.infinispan.factories.KnownComponentNames}
 * and {@link org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory#create(int, int)} for the hardcoded
 * Infinispan default values.
 *
 * @author Radoslav Husar
 * @version February 2015
 */
public enum ThreadPoolResource implements ResourceDefinition, ThreadPoolDefinition {

    ASYNC_OPERATIONS("async-operations", 25, 25, 1000, 60000),
    LISTENER("listener", 1, 1, 100000, 60000),
    REMOTE_COMMAND("remote-command", 25, 25, 100000, 60000),
    STATE_TRANSFER("state-transfer", 1, 60, 0, 60000),
    TRANSPORT("transport", 25, 25, 100000, 60000),
    ;

    static final PathElement WILDCARD_PATH = pathElement(PathElement.WILDCARD_VALUE);

    private static PathElement pathElement(String name) {
        return PathElement.pathElement("thread-pool", name);
    }

    private final String name;
    private final ResourceDescriptionResolver descriptionResolver;
    private final AttributeDefinition minThreads;
    private final AttributeDefinition maxThreads;
    private final AttributeDefinition queueLength;
    private final AttributeDefinition keepAliveTime;

    ThreadPoolResource(String name, int defaultMinThreads, int defaultMaxThreads, int defaultQueueLength, long defaultKeepaliveTime) {
        this.name = name;
        this.descriptionResolver = new InfinispanResourceDescriptionResolver(getPathElement().getKey());
        this.minThreads = createBuilder("min-threads", ModelType.INT, new ModelNode(defaultMinThreads), new IntRangeValidatorBuilder().min(0)).build();
        this.maxThreads = createBuilder("max-threads", ModelType.INT, new ModelNode(defaultMaxThreads), new IntRangeValidatorBuilder().min(0)).build();
        this.queueLength = createBuilder("queue-length", ModelType.INT, new ModelNode(defaultQueueLength), new IntRangeValidatorBuilder().min(0)).build();
        this.keepAliveTime = createBuilder("keepalive-time", ModelType.LONG, new ModelNode(defaultKeepaliveTime), new LongRangeValidatorBuilder().min(0)).build();
    }

    private static SimpleAttributeDefinitionBuilder createBuilder(String name, ModelType type, ModelNode defaultValue, ParameterValidatorBuilder validatorBuilder) {
        return new SimpleAttributeDefinitionBuilder(name, type)
                .setAllowExpression(true)
                .setRequired(false)
                .setDefaultValue(defaultValue)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setMeasurementUnit((type == ModelType.LONG) ? MeasurementUnit.MILLISECONDS : null)
                .setValidator(validatorBuilder.allowExpression(true).allowUndefined(true).build())
                ;
    }

    @Override
    public PathElement getPathElement() {
        return pathElement(this.name);
    }

    @Override
    public DescriptionProvider getDescriptionProvider(ImmutableManagementResourceRegistration registration) {
        return new DefaultResourceDescriptionProvider(registration, this.descriptionResolver);
    }

    @Override
    public void registerOperations(ManagementResourceRegistration registration) {
        ResourceDescriptor descriptor = new ResourceDescriptor(this.descriptionResolver).addAttributes(this.getAttributes());
        ResourceServiceHandler handler = new SimpleResourceServiceHandler<>(new ThreadPoolBuilderFactory(this));
        new AddStepHandler(descriptor, handler).register(registration);
        new RemoveStepHandler(descriptor, handler).register(registration);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration registration) {
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(getAttributes());
        for (AttributeDefinition attribute: getAttributes()) {
            registration.registerReadWriteAttribute(attribute, null, writeHandler);
        }
    }

    @Override
    public void registerNotifications(ManagementResourceRegistration registration) {
        // No-op.
    }

    @Override
    public void registerChildren(ManagementResourceRegistration registration) {
        // No-op.
    }

    @Override
    public List<AccessConstraintDefinition> getAccessConstraints() {
        return Collections.emptyList();
    }

    @Override
    public boolean isRuntime() {
        return false;
    }

    @Override
    public boolean isOrderedChild() {
        return false;
    }

    public void register(ManagementResourceRegistration registration) {
        registration.registerSubModel(this);
    }

    @Override
    public ServiceName getServiceName(String containerName) {
        return CacheContainerServiceName.CONFIGURATION.getServiceName(containerName).append(this.getPathElement().getKeyValuePair());
    }

    @Override
    public AttributeDefinition getMinThreads() {
        return this.minThreads;
    }

    @Override
    public AttributeDefinition getMaxThreads() {
        return this.maxThreads;
    }

    @Override
    public AttributeDefinition getQueueLength() {
        return this.queueLength;
    }

    @Override
    public AttributeDefinition getKeepAliveTime() {
        return this.keepAliveTime;
    }

    Collection<AttributeDefinition> getAttributes() {
        return Arrays.asList(this.minThreads, this.maxThreads, this.queueLength, this.keepAliveTime);
    }
}
