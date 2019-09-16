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

import java.util.HashMap;
import java.util.Map;

import org.infinispan.server.commons.service.Builder;
import org.infinispan.server.commons.service.InjectedValueDependency;
import org.infinispan.server.commons.service.ValueDependency;
import org.infinispan.server.jgroups.spi.ProtocolConfiguration;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceName;
import org.jboss.as.network.SocketBinding;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.value.Value;

/**
 * @author Paul Ferraro
 */
public abstract class AbstractProtocolConfigurationBuilder<P extends ProtocolConfiguration> implements Builder<P>, Value<P>, ProtocolConfiguration {

    final String stackName;
    final String name;

    private final Map<String, String> properties = new HashMap<>();
    private ModuleIdentifier module = ProtocolConfiguration.DEFAULT_MODULE;
    private ValueDependency<SocketBinding> socketBinding;

    public AbstractProtocolConfigurationBuilder(String stackName, String name) {
        this.stackName = stackName;
        this.name = name;
    }

    @Override
    public ServiceName getServiceName() {
        return ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(this.stackName).append(this.name);
    }

    @Override
    public ServiceBuilder<P> build(ServiceTarget target) {
        ServiceBuilder<P> builder = target.addService(this.getServiceName(), new ValueService<>(this));
        if (this.socketBinding != null) {
            this.socketBinding.register(builder);
        }
        return builder.setInitialMode(ServiceController.Mode.ON_DEMAND);
    }

    public AbstractProtocolConfigurationBuilder<P> setModule(ModuleIdentifier module) {
        this.module = module;
        return this;
    }

    public AbstractProtocolConfigurationBuilder<P> setSocketBinding(String socketBindingName) {
        if (socketBindingName != null) {
            this.socketBinding = new InjectedValueDependency<>(SocketBinding.JBOSS_BINDING_NAME.append(socketBindingName), SocketBinding.class);
        }
        return this;
    }

    public AbstractProtocolConfigurationBuilder<P> addProperty(String name, String value) {
        this.properties.put(name, value);
        return this;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getProtocolClassName() {
        StringBuilder builder = new StringBuilder();
        if (module.getName().equals(ProtocolConfiguration.DEFAULT_MODULE.getName()) && !this.name.startsWith(org.jgroups.conf.ProtocolConfiguration.protocol_prefix)) {
            builder.append(org.jgroups.conf.ProtocolConfiguration.protocol_prefix).append('.');
        }
        return builder.append(this.name).toString();
    }

    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(this.properties);
    }

    @Override
    public SocketBinding getSocketBinding() {
        return (this.socketBinding != null) ? this.socketBinding.getValue() : null;
    }

    @Override
    public ModuleIdentifier getModule() {
        return this.module;
    }
}
