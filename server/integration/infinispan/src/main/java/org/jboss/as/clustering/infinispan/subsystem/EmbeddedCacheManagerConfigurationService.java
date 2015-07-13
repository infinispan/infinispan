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

import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.marshall.core.Ids;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.jgroups.spi.ChannelFactory;
import org.jboss.as.clustering.infinispan.ChannelTransport;
import org.jboss.as.clustering.infinispan.MBeanServerProvider;
import org.jboss.as.clustering.infinispan.ManagedExecutorFactory;
import org.jboss.as.clustering.infinispan.ManagedScheduledExecutorFactory;
import org.jboss.as.clustering.infinispan.ThreadPoolExecutorFactories;
import org.jboss.as.clustering.infinispan.io.SimpleExternalizer;
import org.jboss.marshalling.ModularClassResolver;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jgroups.Channel;

import javax.management.MBeanServer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Paul Ferraro
 */
public class EmbeddedCacheManagerConfigurationService implements Service<EmbeddedCacheManagerConfiguration>, EmbeddedCacheManagerConfiguration {

    interface TransportConfiguration {
        Long getLockTimeout();
        ChannelFactory getChannelFactory();
        Channel getChannel();
        Executor getExecutor();
        Executor getTotalOrderExecutor();
        Executor getRemoteCommandExecutor();
        boolean isStrictPeerToPeer();
    }

    interface AuthorizationConfiguration {
        String getPrincipalMapper();
        Map<String, List<String>> getRoles();
    }

    interface Dependencies {
        ModuleLoader getModuleLoader();
        TransportConfiguration getTransportConfiguration();
        AuthorizationConfiguration getAuthorizationConfiguration();
        MBeanServer getMBeanServer();
        Executor getListenerExecutor();
        Executor getAsyncExecutor();
        Executor getStateTransferExecutor();
        ScheduledExecutorService getExpirationExecutor();
        ScheduledExecutorService getReplicationQueueExecutor();
    }

    private final String name;
    private final String defaultCache;
    private final boolean statistics;
    private final Dependencies dependencies;
    private final ModuleIdentifier moduleId;
    private volatile GlobalConfiguration config;

    public EmbeddedCacheManagerConfigurationService(String name, String defaultCache, boolean statistics, ModuleIdentifier moduleIdentifier, Dependencies dependencies) {
        this.name = name;
        this.defaultCache = defaultCache;
        this.statistics = statistics;
        this.moduleId = moduleIdentifier;
        this.dependencies = dependencies;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getDefaultCache() {
        return this.defaultCache;
    }

    @Override
    public GlobalConfiguration getGlobalConfiguration() {
        return this.config;
    }

    @Override
    public ModuleIdentifier getModuleIdentifier() {
        return this.moduleId;
    }

    @Override
    public EmbeddedCacheManagerConfiguration getValue() {
        return this;
    }

    @Override
    public void start(StartContext context) throws StartException {

        GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
        ModuleLoader moduleLoader = this.dependencies.getModuleLoader();
        builder.serialization().classResolver(ModularClassResolver.getInstance(moduleLoader));
        ClassLoader loader = null;
        try {
            loader = (this.moduleId != null) ? moduleLoader.loadModule(this.moduleId).getClassLoader() : EmbeddedCacheManagerConfiguration.class.getClassLoader();
            builder.classLoader(loader);
            int id = Ids.MAX_ID;
            for (SimpleExternalizer<?> externalizer: ServiceFinder.load(SimpleExternalizer.class, loader)) {
                builder.serialization().addAdvancedExternalizer(id++, externalizer);
            }
        } catch (ModuleLoadException e) {
            throw new StartException(e);
        }
        builder.shutdown().hookBehavior(ShutdownHookBehavior.DONT_REGISTER);

        TransportConfiguration transport = this.dependencies.getTransportConfiguration();
        TransportConfigurationBuilder transportBuilder = builder.transport();

        if (transport != null) {
            transportBuilder.transport(new ChannelTransport(transport.getChannel(), transport.getChannelFactory()));
            Long timeout = transport.getLockTimeout();
            if (timeout != null) {
                transportBuilder.distributedSyncTimeout(timeout.longValue());
            }
            // Topology is retrieved from the channel
            org.infinispan.server.jgroups.spi.TransportConfiguration.Topology topology = transport.getChannelFactory().getProtocolStackConfiguration().getTransport().getTopology();
            if (topology != null) {
                String site = topology.getSite();
                if (site != null) {
                    transportBuilder.siteId(site);
                }
                String rack = topology.getRack();
                if (rack != null) {
                    transportBuilder.rackId(rack);
                }
                String machine = topology.getMachine();
                if (machine != null) {
                    transportBuilder.machineId(machine);
                }
            }
            transportBuilder.clusterName(this.name);

            Executor executor = transport.getExecutor();
            if (executor != null) {
                builder.transport().transportThreadPool().threadPoolFactory(new ManagedExecutorFactory(executor));
            }
            Executor totalOrderExecutor = transport.getTotalOrderExecutor();
            if (totalOrderExecutor != null) {
                builder.transport().totalOrderThreadPool().threadPoolFactory(new ManagedExecutorFactory(totalOrderExecutor));
           }
           Executor remoteCommandExecutor = transport.getRemoteCommandExecutor();
           if (remoteCommandExecutor != null) {
                builder.transport().remoteCommandThreadPool().threadPoolFactory(new ManagedExecutorFactory(remoteCommandExecutor));
           }
        }

        AuthorizationConfiguration authorization = this.dependencies.getAuthorizationConfiguration();
        GlobalAuthorizationConfigurationBuilder authorizationBuilder = builder.security().authorization();

        if (authorization != null) {
            authorizationBuilder.enable();
            if (authorization.getPrincipalMapper() != null) {
                try {
                    authorizationBuilder.principalRoleMapper(Class.forName(authorization.getPrincipalMapper(), true, loader).asSubclass(PrincipalRoleMapper.class).newInstance());
                } catch (Exception e) {
                    throw new StartException(e);
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

        Executor listenerExecutor = this.dependencies.getListenerExecutor();
        if (listenerExecutor != null) {
            builder.listenerThreadPool().threadPoolFactory(new ManagedExecutorFactory(listenerExecutor));
        }
        Executor asyncExecutor = this.dependencies.getAsyncExecutor();
        if (asyncExecutor != null) {
            builder.asyncThreadPool().threadPoolFactory(
                  ThreadPoolExecutorFactories.mkManagedExecutorFactory(asyncExecutor));
        }
        ScheduledExecutorService expirationExecutor = this.dependencies.getExpirationExecutor();
        if (expirationExecutor != null) {
            builder.expirationThreadPool().threadPoolFactory(new ManagedScheduledExecutorFactory(expirationExecutor));
        }
        ScheduledExecutorService replicationQueueExecutor = this.dependencies.getReplicationQueueExecutor();
        if (replicationQueueExecutor != null) {
            builder.replicationQueueThreadPool().threadPoolFactory(new ManagedScheduledExecutorFactory(replicationQueueExecutor));
        }
        Executor stateTransferExecutor = this.dependencies.getStateTransferExecutor();
        if (stateTransferExecutor != null) {
            builder.stateTransferThreadPool().threadPoolFactory(new ManagedExecutorFactory(stateTransferExecutor));
        }

        GlobalJmxStatisticsConfigurationBuilder jmxBuilder = builder.globalJmxStatistics().cacheManagerName(this.name);
        jmxBuilder.jmxDomain(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(null).getCanonicalName());

        MBeanServer server = this.dependencies.getMBeanServer();
        if (server != null && this.statistics) {
            jmxBuilder.enable()
                .mBeanServerLookup(new MBeanServerProvider(server))
                .allowDuplicateDomains(true)
            ;
        } else {
            jmxBuilder.disable();
        }
        this.config = builder.build();
    }

    @Override
    public void stop(StopContext context) {
        // Nothing to stop
    }
}
