package org.jboss.as.clustering.infinispan.subsystem;

import java.util.concurrent.TimeUnit;

import org.infinispan.server.commons.service.Builder;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.infinispan.server.jgroups.spi.service.ChannelServiceName;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.TransportConfiguration;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;
import org.jgroups.JChannel;

public class TransportConfigurationBuilder implements Builder<TransportConfiguration>, Value<TransportConfiguration>, TransportConfiguration {
    private final InjectedValue<JChannel> channel = new InjectedValue<>();
    private final InjectedValue<ChannelFactory> factory = new InjectedValue<>();
    private final String name;

    private Long lockTimeout;
    private boolean strictPeerToPeer;
    private int initialClusterSize;
    private long initialClusterTimeout;

    public TransportConfigurationBuilder(String name) {
        this.name = name;
    }

    TransportConfigurationBuilder setLockTimeout(long lockTimeout, TimeUnit timeUnit) {
        this.lockTimeout = timeUnit.toMillis(lockTimeout);
        return this;
    }

    TransportConfigurationBuilder setStrictPeerToPeer(boolean strictPeerToPeer) {
        this.strictPeerToPeer = strictPeerToPeer;
        return this;
    }

    TransportConfigurationBuilder setInitialClusterSize(int initialClusterSize) {
        this.initialClusterSize = initialClusterSize;
        return this;
    }

    TransportConfigurationBuilder setInitialClusterTimeout(long initialClusterTimeout) {
        this.initialClusterTimeout = initialClusterTimeout;
        return this;
    }

    @Override
    public ChannelFactory getChannelFactory() {
        return this.factory.getValue();
    }

    @Override
    public JChannel getChannel() {
        return this.channel.getValue();
    }

   @Override
    public boolean isStrictPeerToPeer() {
        return this.strictPeerToPeer;
    }

    @Override
    public Long getLockTimeout() {
        return this.lockTimeout;
    }

    @Override
    public int getInitialClusterSize() {
       return this.initialClusterSize;
    }

    @Override
    public long getInitialClusterTimeout() {
       return this.initialClusterTimeout;
    }

    @Override
    public TransportConfiguration getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

    @Override
    public ServiceName getServiceName() {
        return CacheContainerServiceName.CACHE_CONTAINER.getServiceName(this.name).append("transport");
    }

    @Override
    public ServiceBuilder<TransportConfiguration> build(ServiceTarget target) {
        ServiceBuilder<TransportConfiguration> builder = target.addService(this.getServiceName(), new ValueService<>(this))
                .addDependency(ChannelServiceName.CHANNEL.getServiceName(this.name), JChannel.class, this.channel)
                .addDependency(ChannelServiceName.FACTORY.getServiceName(this.name), ChannelFactory.class, this.factory)
        ;
        return builder.setInitialMode(ServiceController.Mode.ON_DEMAND);
    }

}
