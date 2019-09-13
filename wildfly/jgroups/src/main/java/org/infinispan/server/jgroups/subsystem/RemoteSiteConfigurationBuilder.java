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

import org.infinispan.server.commons.service.Builder;
import org.infinispan.server.jgroups.spi.RemoteSiteConfiguration;
import org.infinispan.server.jgroups.spi.service.ChannelServiceName;
import org.infinispan.server.jgroups.spi.service.ProtocolStackServiceName;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;
import org.jgroups.JChannel;

/**
 * @author Paul Ferraro
 */
public class RemoteSiteConfigurationBuilder implements Builder<RemoteSiteConfiguration>, Value<RemoteSiteConfiguration>, RemoteSiteConfiguration {

    private final InjectedValue<JChannel> channel = new InjectedValue<>();
    private final String stackName;
    private final String siteName;
    private String channelName;

    public RemoteSiteConfigurationBuilder(String stackName, String siteName) {
        this.stackName = stackName;
        this.siteName = siteName;
    }

    @Override
    public ServiceName getServiceName() {
        return ProtocolStackServiceName.CHANNEL_FACTORY.getServiceName(this.stackName).append("relay", this.siteName);
    }

    @Override
    public ServiceBuilder<RemoteSiteConfiguration> build(ServiceTarget target) {
        return target.addService(this.getServiceName(), new ValueService<>(this))
                .addDependency(ChannelServiceName.CHANNEL.getServiceName(this.channelName), JChannel.class, this.channel)
                .setInitialMode(ServiceController.Mode.ON_DEMAND)
        ;
    }

    @Override
    public RemoteSiteConfiguration getValue() {
        return this;
    }

    public RemoteSiteConfigurationBuilder setChannel(String channelName) {
        this.channelName = channelName;
        return this;
    }

    @Override
    public String getName() {
        return this.siteName;
    }

    @Override
    public JChannel getChannel() {
        return this.channel.getValue();
    }

    @Override
    public String getClusterName() {
        return this.channelName;
    }
}
