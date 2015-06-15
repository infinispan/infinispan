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
package org.jboss.as.clustering.infinispan.subsystem;

import java.util.List;
import java.util.ServiceLoader;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;

import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.marshall.core.Ids;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.server.commons.service.Builder;
import org.infinispan.server.commons.service.InjectedValueDependency;
import org.infinispan.server.commons.service.ValueDependency;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.infinispan.spi.service.CacheServiceNameFactory;
import org.infinispan.server.jgroups.spi.ProtocolStackConfiguration;
import org.infinispan.server.jgroups.spi.RelayConfiguration;
import org.jboss.as.clustering.infinispan.ChannelTransport;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.as.clustering.infinispan.MBeanServerProvider;
import org.jboss.as.clustering.infinispan.ManagedExecutorFactory;
import org.jboss.as.clustering.infinispan.ManagedScheduledExecutorFactory;
import org.jboss.as.clustering.infinispan.io.SimpleExternalizer;
import org.jboss.as.jmx.MBeanServerService;
import org.jboss.as.server.Services;
import org.jboss.as.threads.ThreadsServices;
import org.jboss.marshalling.ModularClassResolver;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.ValueService;
import org.jboss.msc.value.InjectedValue;
import org.jboss.msc.value.Value;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.AuthorizationConfiguration;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.TransportConfiguration;

/**
 * @author Paul Ferraro
 */
public class CacheContainerConfigurationBuilder implements Builder<GlobalConfiguration>, Value<GlobalConfiguration> {

    private final InjectedValue<ModuleLoader> loader = new InjectedValue<>();
    private final InjectedValue<MBeanServer> server = new InjectedValue<>();
    private final String name;
    private boolean statisticsEnabled;
    private ModuleIdentifier module;
    private AuthorizationConfigurationBuilder authorization = null;
    private ValueDependency<TransportConfiguration> transport = null;
    private ValueDependency<Executor> asyncExecutor = null;
    private ValueDependency<Executor> listenerExecutor = null;
    private ValueDependency<ScheduledExecutorService> expirationExecutor = null;
    private ValueDependency<ScheduledExecutorService> replicationQueueExecutor = null;

    public CacheContainerConfigurationBuilder(String name) {
        this.name = name;
    }

    @Override
    public ServiceName getServiceName() {
        return CacheContainerServiceName.CONFIGURATION.getServiceName(this.name);
    }

    @Override
    public ServiceBuilder<GlobalConfiguration> build(ServiceTarget target) {
        ServiceBuilder<GlobalConfiguration> builder = target.addService(this.getServiceName(), new ValueService<>(this))
                .addDependency(Services.JBOSS_SERVICE_MODULE_LOADER, ModuleLoader.class, this.loader)
                .addDependency(MBeanServerService.SERVICE_NAME, MBeanServer.class, this.server)
        ;
        if (this.transport != null) {
            this.transport.register(builder);
        }
        if (this.asyncExecutor != null) {
            this.asyncExecutor.register(builder);
        }
        if (this.listenerExecutor != null) {
            this.listenerExecutor.register(builder);
        }
        if (this.expirationExecutor != null) {
            this.expirationExecutor.register(builder);
        }
        if (this.replicationQueueExecutor != null) {
            this.replicationQueueExecutor.register(builder);
        }
        return builder.setInitialMode(ServiceController.Mode.ON_DEMAND);
    }

    @Override
    public GlobalConfiguration getValue() {

        GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
        ModuleLoader moduleLoader = this.loader.getValue();
        builder.serialization().classResolver(ModularClassResolver.getInstance(moduleLoader));
        ClassLoader loader = null;
        try {
            loader = (this.module != null) ? moduleLoader.loadModule(this.module).getClassLoader() : CacheContainerConfiguration.class.getClassLoader();
            builder.classLoader(loader);
            int id = Ids.MAX_ID;
            for (SimpleExternalizer<?> externalizer: ServiceLoader.load(SimpleExternalizer.class, loader)) {
                InfinispanLogger.ROOT_LOGGER.debugf("Cache container %s will use an externalizer for %s", this.name, externalizer.getTargetClass().getName());
                builder.serialization().addAdvancedExternalizer(id++, externalizer);
            }
        } catch (ModuleLoadException e) {
            throw new IllegalStateException(e);
        }
        builder.shutdown().hookBehavior(ShutdownHookBehavior.DONT_REGISTER);

        AuthorizationConfiguration authorization = (this.authorization != null) ? this.authorization.getValue() : null;
        GlobalAuthorizationConfigurationBuilder authorizationBuilder = builder.security().authorization();

        if (authorization != null) {
            authorizationBuilder.enable();
            if (authorization.getPrincipalMapper() != null) {
                try {
                    authorizationBuilder.principalRoleMapper(Class.forName(authorization.getPrincipalMapper(), true, loader).asSubclass(PrincipalRoleMapper.class).newInstance());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            } else {
                authorizationBuilder.principalRoleMapper(new ClusterRoleMapper());
            }
            for(Entry<String, List<String>> role : authorization.getRoles().entrySet()) {
                GlobalRoleConfigurationBuilder roleBuilder = authorizationBuilder.role(role.getKey());
                for(String perm : role.getValue()) {
                    roleBuilder.permission(perm);
                }
            }
        }

        TransportConfiguration transport = (this.transport != null) ? this.transport.getValue() : null;

        if (transport != null) {
            org.infinispan.configuration.global.TransportConfigurationBuilder transportBuilder = builder.transport()
                    .clusterName(this.name)
                    .transport(new ChannelTransport(transport.getChannel(), transport.getChannelFactory()))
                    .distributedSyncTimeout(transport.getLockTimeout())
            ;

            // Topology is retrieved from the channel
            ProtocolStackConfiguration stack = transport.getChannelFactory().getProtocolStackConfiguration();
            org.infinispan.server.jgroups.spi.TransportConfiguration.Topology topology = stack.getTransport().getTopology();
            if (topology != null) {
                transportBuilder.siteId(topology.getSite()).rackId(topology.getRack()).machineId(topology.getMachine());
            }

            Executor executor = transport.getExecutor();
            if (executor != null) {
                transportBuilder.transportThreadPool().threadPoolFactory(new ManagedExecutorFactory(executor));
            }

            RelayConfiguration relay = stack.getRelay();
            if (relay != null) {
                builder.site().localSite(relay.getSiteName());
            }
        }

        Executor asyncExecutor = (this.asyncExecutor != null) ? this.asyncExecutor.getValue() : null;
        if (asyncExecutor != null) {
            builder.asyncThreadPool().threadPoolFactory(new ManagedExecutorFactory(asyncExecutor));
        }
        Executor listenerExecutor = (this.listenerExecutor != null) ? this.listenerExecutor.getValue() : null;
        if (listenerExecutor != null) {
            builder.listenerThreadPool().threadPoolFactory(new ManagedExecutorFactory(listenerExecutor));
        }
        ScheduledExecutorService expirationExecutor = (this.expirationExecutor != null) ? this.expirationExecutor.getValue() : null;
        if (expirationExecutor != null) {
            builder.expirationThreadPool().threadPoolFactory(new ManagedScheduledExecutorFactory(expirationExecutor));
        }
        ScheduledExecutorService replicationQueueExecutor = (this.replicationQueueExecutor != null) ? this.replicationQueueExecutor.getValue() : null;
        if (replicationQueueExecutor != null) {
            builder.replicationQueueThreadPool().threadPoolFactory(new ManagedExecutorFactory(replicationQueueExecutor));
        }

        builder.globalJmxStatistics()
                .enabled(this.statisticsEnabled)
                .cacheManagerName(this.name)
                .mBeanServerLookup(new MBeanServerProvider(this.server.getValue()))
                .jmxDomain(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(CacheServiceNameFactory.DEFAULT_CACHE).getParent().getCanonicalName())
                .allowDuplicateDomains(true);

        return builder.build();
    }

    public CacheContainerConfigurationBuilder setModule(ModuleIdentifier module) {
        this.module = module;
        return this;
    }

    public CacheContainerConfigurationBuilder setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    public TransportConfigurationBuilder setTransport() {
        TransportConfigurationBuilder builder = new TransportConfigurationBuilder(this.name);
        this.transport = new InjectedValueDependency<>(builder, TransportConfiguration.class);
        return builder;
    }

    public AuthorizationConfigurationBuilder setAuthorization() {
        this.authorization = new AuthorizationConfigurationBuilder();
        return this.authorization;
    }

    public CacheContainerConfigurationBuilder setAsyncExecutor(String executorName) {
        if (executorName != null) {
            this.asyncExecutor = new InjectedValueDependency<>(ThreadsServices.executorName(executorName), Executor.class);
        }
        return this;
    }

    public CacheContainerConfigurationBuilder setListenerExecutor(String executorName) {
        if (executorName != null) {
            this.listenerExecutor = new InjectedValueDependency<>(ThreadsServices.executorName(executorName), Executor.class);
        }
        return this;
    }

    public CacheContainerConfigurationBuilder setExpirationExecutor(String executorName) {
        if (executorName != null) {
            this.expirationExecutor = new InjectedValueDependency<>(ThreadsServices.executorName(executorName), ScheduledExecutorService.class);
        }
        return this;
    }

    public CacheContainerConfigurationBuilder setReplicationQueueExecutor(String executorName) {
        if (executorName != null) {
            this.replicationQueueExecutor = new InjectedValueDependency<>(ThreadsServices.executorName(executorName), ScheduledExecutorService.class);
        }
        return this;
    }
}
