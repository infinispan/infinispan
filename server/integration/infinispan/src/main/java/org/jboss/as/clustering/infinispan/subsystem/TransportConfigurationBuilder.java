package org.jboss.as.clustering.infinispan.subsystem;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.commons.service.Builder;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.infinispan.server.jgroups.spi.service.ChannelServiceName;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.TransportConfiguration;
import org.jboss.msc.inject.Injector;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;
import org.jgroups.Channel;

public class TransportConfigurationBuilder implements Builder<TransportConfiguration>, Value<TransportConfiguration>, TransportConfiguration {
    private final InjectedValue<Channel> channel = new InjectedValue<>();
    private final InjectedValue<ChannelFactory> factory = new InjectedValue<>();
    private final InjectedValue<Executor> executor = new InjectedValue<>();
    private final InjectedValue<Executor> totalOrderExecutor = new InjectedValue<>();
    private final InjectedValue<Executor> remoteCommandExecutor = new InjectedValue<>();
    private final String name;

    private Long lockTimeout;
    private boolean strictPeerToPeer;

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

    public TransportConfigurationBuilder setExecutor(String name) {
        return this;
    }

    public TransportConfigurationBuilder setRemoteCommandExecutor(String name) {
        return this;
    }

    @Deprecated
    public TransportConfigurationBuilder setTotalOrderExecutor(String name) {
        return this;
    }

    Injector<Executor> getExecutorInjector() {
        return this.executor;
    }

    Injector<Executor> getTotalorderExecutorInjector() {
        return this.totalOrderExecutor;
    }

    Injector<Executor> getRemoteCommandExecutorInjector() {
        return this.remoteCommandExecutor;
    }

    @Override
    public ChannelFactory getChannelFactory() {
        return this.factory.getValue();
    }

    @Override
    public Channel getChannel() {
        return this.channel.getValue();
    }

    @Override
    public Executor getExecutor() {
        return this.executor.getOptionalValue();
    }

    @Override
    public Executor getTotalOrderExecutor() {
        return this.totalOrderExecutor.getOptionalValue();
    }

    @Override
    public Executor getRemoteCommandExecutor() {
        return this.remoteCommandExecutor.getOptionalValue();
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
                .addDependency(ChannelServiceName.CHANNEL.getServiceName(this.name), Channel.class, this.channel)
                .addDependency(ChannelServiceName.FACTORY.getServiceName(this.name), ChannelFactory.class, this.factory)
        ;
        return builder.setInitialMode(ServiceController.Mode.ON_DEMAND);
    }

}