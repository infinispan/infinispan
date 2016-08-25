/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
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
package org.infinispan.server.jgroups.subsystem;

import static org.infinispan.server.jgroups.logging.JGroupsLogger.ROOT_LOGGER;

import org.infinispan.server.commons.dmr.ModelNodes;
import org.infinispan.server.commons.naming.BinderServiceBuilder;
import org.infinispan.server.commons.service.AliasServiceBuilder;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.infinispan.server.jgroups.spi.service.ChannelServiceName;
import org.infinispan.server.jgroups.spi.service.ChannelServiceNameFactory;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceName;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceNameFactory;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceTarget;
import org.jgroups.JChannel;

/**
 * Handler for JGroups subsystem add operations.
 *
 * @author Paul Ferraro
 * @author Richard Achmatowicz (c) 2011 Red Hat, Inc
 */
public class JGroupsSubsystemAddHandler extends AbstractAddStepHandler {

    JGroupsSubsystemAddHandler() {
        super(JGroupsSubsystemResourceDefinition.ATTRIBUTES);
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {

        ROOT_LOGGER.activatingSubsystem();

        installRuntimeServices(context, operation, model);
    }

    static void installRuntimeServices(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        ServiceTarget target = context.getServiceTarget();

        new ProtocolDefaultsBuilder().build(target).install();

        String defaultChannel = ModelNodes.asString(JGroupsSubsystemResourceDefinition.DEFAULT_CHANNEL.resolveModelAttribute(context, model), ChannelServiceNameFactory.DEFAULT_CHANNEL);
        if (!defaultChannel.equals(ChannelServiceNameFactory.DEFAULT_CHANNEL)) {
            for (ChannelServiceNameFactory factory : ChannelServiceName.values()) {
                new AliasServiceBuilder<>(factory.getServiceName(), factory.getServiceName(defaultChannel), Object.class).build(target).install();
            }
            new BinderServiceBuilder<>(JGroupsBindingFactory.createChannelBinding(ChannelServiceNameFactory.DEFAULT_CHANNEL), ChannelServiceName.CHANNEL.getServiceName(defaultChannel), JChannel.class).build(target).install();

            new AliasServiceBuilder<>(ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(ChannelServiceNameFactory.DEFAULT_CHANNEL), ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(defaultChannel), ChannelFactory.class).build(target).install();
            new BinderServiceBuilder<>(JGroupsBindingFactory.createChannelFactoryBinding(ChannelServiceNameFactory.DEFAULT_CHANNEL), ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(defaultChannel), ChannelFactory.class).build(target).install();
        }

        String defaultStack = ModelNodes.asString(JGroupsSubsystemResourceDefinition.DEFAULT_STACK.resolveModelAttribute(context, model), ProtocolStackServiceNameFactory.DEFAULT_STACK);
        if (!defaultStack.equals(ProtocolStackServiceNameFactory.DEFAULT_STACK)) {
            new AliasServiceBuilder<>(ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(), ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(defaultStack), ChannelFactory.class).build(target).install();
        }
    }

    static void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        // remove the ProtocolDefaultsService
        context.removeService(ProtocolDefaultsBuilder.SERVICE_NAME);

        String defaultStack = ModelNodes.asString(JGroupsSubsystemResourceDefinition.DEFAULT_STACK.resolveModelAttribute(context, model));
        if ((defaultStack != null) && !defaultStack.equals(ProtocolStackServiceNameFactory.DEFAULT_STACK)) {
            context.removeService(ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName());
        }

        String defaultChannel = ModelNodes.asString(JGroupsSubsystemResourceDefinition.DEFAULT_CHANNEL.resolveModelAttribute(context, model));
        if ((defaultChannel != null) && !defaultChannel.equals(ChannelServiceNameFactory.DEFAULT_CHANNEL)) {

            context.removeService(JGroupsBindingFactory.createChannelFactoryBinding(ChannelServiceNameFactory.DEFAULT_CHANNEL).getBinderServiceName());
            context.removeService(ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(ChannelServiceNameFactory.DEFAULT_CHANNEL));

            context.removeService(JGroupsBindingFactory.createChannelBinding(ChannelServiceNameFactory.DEFAULT_CHANNEL).getBinderServiceName());

            for (ChannelServiceNameFactory factory : ChannelServiceName.values()) {
                context.removeService(factory.getServiceName());
            }
        }
    }
}
