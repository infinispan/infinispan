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
import java.util.Map.Entry;
import java.util.ServiceLoader;

import javax.management.MBeanServer;

import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.configuration.global.GlobalStateConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.configuration.global.ThreadPoolConfiguration;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.marshall.core.Ids;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.security.impl.NullAuditLogger;
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
import org.jboss.as.clustering.infinispan.io.SimpleExternalizer;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.AuthorizationConfiguration;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.GlobalStateLocationConfiguration;
import org.jboss.as.clustering.infinispan.subsystem.EmbeddedCacheManagerConfigurationService.TransportConfiguration;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.controller.services.path.PathManagerService;
import org.jboss.as.jmx.MBeanServerService;
import org.jboss.as.server.Services;
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
    private GlobalStateLocationConfigurationBuilder globalStateLocation = null;
    private final InjectedValue<ThreadPoolConfiguration> asyncOperationsThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> expirationThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> listenerThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> persistenceThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> remoteCommandThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> stateTransferThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> transportThreadPool = new InjectedValue<>();
    private final InjectedValue<ThreadPoolConfiguration> replicationQueueThreadPool = new InjectedValue<>();
    private final InjectedValue<PathManager> pathManager = new InjectedValue<>();

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
                .addDependency(ThreadPoolResource.ASYNC_OPERATIONS.getServiceName(this.name), ThreadPoolConfiguration.class, this.asyncOperationsThreadPool)
                .addDependency(ThreadPoolResource.LISTENER.getServiceName(this.name), ThreadPoolConfiguration.class, this.listenerThreadPool)
                .addDependency(ThreadPoolResource.REMOTE_COMMAND.getServiceName(this.name), ThreadPoolConfiguration.class, this.remoteCommandThreadPool)
                .addDependency(ThreadPoolResource.STATE_TRANSFER.getServiceName(this.name), ThreadPoolConfiguration.class, this.stateTransferThreadPool)
                .addDependency(ThreadPoolResource.PERSISTENCE.getServiceName(this.name), ThreadPoolConfiguration.class, this.persistenceThreadPool)
                .addDependency(ThreadPoolResource.TRANSPORT.getServiceName(this.name), ThreadPoolConfiguration.class, this.transportThreadPool)
                .addDependency(ScheduledThreadPoolResource.EXPIRATION.getServiceName(this.name), ThreadPoolConfiguration.class, this.expirationThreadPool)
                .addDependency(ScheduledThreadPoolResource.REPLICATION_QUEUE.getServiceName(this.name), ThreadPoolConfiguration.class, this.replicationQueueThreadPool)
        ;
        if (this.transport != null) {
            this.transport.register(builder);
        }
        if (this.globalStateLocation != null) {
            builder.addDependency(PathManagerService.SERVICE_NAME, PathManager.class, this.pathManager);
        }
        return builder.setInitialMode(ServiceController.Mode.ON_DEMAND);
    }

    @Override
    public GlobalConfiguration getValue() {

        GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
        ModuleLoader moduleLoader = this.loader.getValue();
        builder.serialization().classResolver(ModularClassResolver.getInstance(moduleLoader));
        ClassLoader loader;
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
            if (authorization.getAuditLogger() != null) {
               try {
                  authorizationBuilder.auditLogger(Class.forName(authorization.getAuditLogger(), true, loader).asSubclass(AuditLogger.class).newInstance());
               } catch (Exception e) {
                  throw new IllegalStateException(e);
               }
            } else {
               authorizationBuilder.auditLogger(new NullAuditLogger());
            }
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


            transportBuilder.transportThreadPool().read(this.transportThreadPool.getValue());
            transportBuilder.remoteCommandThreadPool().read(this.remoteCommandThreadPool.getValue());

            RelayConfiguration relay = stack.getRelay();
            if (relay != null) {
                builder.site().localSite(relay.getSiteName());
            }
        }

        GlobalStateLocationConfiguration statePersistence = (this.globalStateLocation != null) ? this.globalStateLocation.getValue() : null;
        if (statePersistence != null) {
            GlobalStateConfigurationBuilder statePersistenceBuilder = builder.globalState().enable();
            String persistentLocation = pathManager.getValue().resolveRelativePathEntry(statePersistence.getPersistencePath(), statePersistence.getPersistenceRelativeTo());
            statePersistenceBuilder.persistentLocation(persistentLocation);
            String temporaryLocation = pathManager.getValue().resolveRelativePathEntry(statePersistence.getPersistencePath(), statePersistence.getPersistenceRelativeTo());
            statePersistenceBuilder.temporaryLocation(temporaryLocation);
        }

        builder.asyncThreadPool().read(this.asyncOperationsThreadPool.getValue());
        builder.expirationThreadPool().read(this.expirationThreadPool.getValue());
        builder.listenerThreadPool().read(this.listenerThreadPool.getValue());
        builder.stateTransferThreadPool().read(this.stateTransferThreadPool.getValue());
        builder.persistenceThreadPool().read(this.persistenceThreadPool.getValue());
        builder.replicationQueueThreadPool().read(this.replicationQueueThreadPool.getValue());

        builder.globalJmxStatistics()
                .enabled(this.statisticsEnabled)
                .cacheManagerName(this.name)
                .mBeanServerLookup(new MBeanServerProvider(this.server.getValue()))
                .jmxDomain(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(CacheServiceNameFactory.DEFAULT_CACHE).getParent().getCanonicalName())
                .allowDuplicateDomains(true);

        builder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
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

    public GlobalStateLocationConfigurationBuilder setGlobalState() {
        this.globalStateLocation = new GlobalStateLocationConfigurationBuilder();
        return this.globalStateLocation;
    }
}
