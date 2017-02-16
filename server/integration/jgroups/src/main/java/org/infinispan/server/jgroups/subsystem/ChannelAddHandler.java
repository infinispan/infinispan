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
package org.infinispan.server.jgroups.subsystem;

import org.infinispan.server.commons.dmr.ModelNodes;
import org.infinispan.server.commons.naming.BinderServiceBuilder;
import org.infinispan.server.commons.service.AliasServiceBuilder;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.infinispan.server.jgroups.spi.service.ChannelBuilder;
import org.infinispan.server.jgroups.spi.service.ChannelConnectorBuilder;
import org.infinispan.server.jgroups.spi.service.ChannelServiceName;
import org.infinispan.server.jgroups.spi.service.ChannelServiceNameFactory;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceName;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceNameFactory;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.RunningMode;
import org.jboss.as.controller.registry.Resource;
import org.jboss.dmr.ModelNode;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.msc.service.ServiceTarget;
import org.jgroups.JChannel;

/**
 * Handler for /subsystem=jgroups/channel=*:add() operations
 * @author Paul Ferraro
 */
public class ChannelAddHandler extends AbstractAddStepHandler {

    private final boolean allowRuntimeOnlyRegistration;

    ChannelAddHandler(boolean allowRuntimeOnlyRegistration) {
        super(ChannelResourceDefinition.ATTRIBUTES);
        this.allowRuntimeOnlyRegistration = allowRuntimeOnlyRegistration;
    }

    @Override
    protected void populateModel(OperationContext context, ModelNode operation, Resource resource) throws OperationFailedException {
        super.populateModel(context, operation, resource);

        // Register runtime resource children for channel protocols
        if (this.allowRuntimeOnlyRegistration && (context.getRunningMode() == RunningMode.NORMAL)) {
            String name = context.getCurrentAddressValue();
            String stack = ModelNodes.asString(ChannelResourceDefinition.STACK.resolveModelAttribute(context, resource.getModel()));

            PathAddress address = context.getCurrentAddress();
            PathAddress subsystemAddress = address.subAddress(0, address.size() - 1);
            // Lookup the name of the default stack if necessary
            PathAddress stackAddress = subsystemAddress.append(StackResourceDefinition.pathElement((stack != null) ? stack : JGroupsSubsystemResourceDefinition.DEFAULT_STACK.resolveModelAttribute(context, context.readResourceFromRoot(subsystemAddress, false).getModel()).asString()));

            context.addStep(new ProtocolResourceRegistrationHandler(name, stackAddress), OperationContext.Stage.MODEL);
        }
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {
        installRuntimeServices(context, operation, model);
    }

    static void installRuntimeServices(OperationContext context, ModelNode operation, ModelNode model) throws OperationFailedException {

        String name = context.getCurrentAddressValue();
        String stack = ModelNodes.asString(ChannelResourceDefinition.STACK.resolveModelAttribute(context, model), ProtocolStackServiceNameFactory.DEFAULT_STACK);

        ModuleIdentifier module = ModelNodes.asModuleIdentifier(ChannelResourceDefinition.MODULE.resolveModelAttribute(context, model));

        ServiceTarget target = context.getServiceTarget();

        // Install channel factory alias
        new AliasServiceBuilder<>(ChannelServiceName.FACTORY.getServiceName(name), ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(stack), ChannelFactory.class).build(target).install();

        // Install channel
        new ChannelBuilder(name).build(target).install();

        // Install channel connector
        new ChannelConnectorBuilder(name).build(target).install();

        // Install channel jndi binding
        new BinderServiceBuilder<>(JGroupsBindingFactory.createChannelBinding(name), ChannelServiceName.CHANNEL.getServiceName(name), JChannel.class).build(target).install();

        // Install fork channel factory
        new ForkChannelFactoryBuilder(name).build(target).install();

        // Install fork channel factory jndi binding
        new BinderServiceBuilder<>(JGroupsBindingFactory.createChannelFactoryBinding(name), ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(name), ChannelFactory.class).build(target).install();
    }

    static void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode model) {
        String name = context.getCurrentAddressValue();

        context.removeService(JGroupsBindingFactory.createChannelBinding(name).getBinderServiceName());

        for (ChannelServiceNameFactory factory : ChannelServiceName.values()) {
            context.removeService(factory.getServiceName(name));
        }

        context.removeService(JGroupsBindingFactory.createChannelFactoryBinding(name).getBinderServiceName());
        context.removeService(ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(name));
    }
}
