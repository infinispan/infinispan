package org.jboss.as.clustering.infinispan.subsystem;

import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.modules.ModuleLoader;
import org.jboss.msc.inject.Injector;
import org.jboss.msc.value.InjectedValue;

/**
 * CacheConfigurationDependencies.
 *
 * @author Paul Ferraro
 * @author Tristan Tarrant
 * @since 8.0
 */
class CacheConfigurationDependencies implements CacheConfigurationService.Dependencies {

    private final InjectedValue<EmbeddedCacheManager> container = new InjectedValue<EmbeddedCacheManager>();
    private final InjectedValue<TransactionManager> tm = new InjectedValue<TransactionManager>();
    private final InjectedValue<TransactionSynchronizationRegistry> tsr = new InjectedValue<TransactionSynchronizationRegistry>();
    private final InjectedValue<ModuleLoader> moduleLoader = new InjectedValue<ModuleLoader>();
    private final InjectedValue<Configuration> parentConfiguration = new InjectedValue<Configuration>();

    CacheConfigurationDependencies() {
    }

    Injector<EmbeddedCacheManager> getContainerInjector() {
        return container;
    }

    Injector<TransactionManager> getTransactionManagerInjector() {
        return this.tm;
    }

    Injector<TransactionSynchronizationRegistry> getTransactionSynchronizationRegistryInjector() {
        return this.tsr;
    }

    Injector<ModuleLoader> getModuleLoaderInjector() {
        return this.moduleLoader;
    }

    Injector<Configuration> getParentConfigurationInjector() {
        return this.parentConfiguration;
    }

    @Override
    public EmbeddedCacheManager getCacheContainer() {
        return this.container.getValue();
    }

    @Override
    public TransactionManager getTransactionManager() {
        return this.tm.getOptionalValue();
    }

    @Override
    public TransactionSynchronizationRegistry getTransactionSynchronizationRegistry() {
        return this.tsr.getOptionalValue();
    }

    @Override
    public ModuleLoader getModuleLoader() {
        return this.moduleLoader.getValue();
    }

    @Override
    public Configuration getParentConfiguration() {
        return this.parentConfiguration.getOptionalValue();
    }
}
