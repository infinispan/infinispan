package org.infinispan.quarkus.embedded.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.configuration.cache.TransactionConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

@ApplicationScoped
public class InfinispanEmbeddedProducer {

    private volatile InfinispanEmbeddedRuntimeConfig config;

    public void setRuntimeConfig(InfinispanEmbeddedRuntimeConfig config) {
        this.config = config;
    }

    @Singleton
    @Produces
    EmbeddedCacheManager manager() {
        if (config.xmlConfig.isPresent()) {
            String configurationFile = config.xmlConfig.get();
            try {
                InputStream configurationStream = FileLookupFactory.newInstance().lookupFileStrict(configurationFile,
                        Thread.currentThread().getContextClassLoader());
                ConfigurationBuilderHolder configHolder = new ParserRegistry().parse(configurationStream, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML);
                verifyTransactionConfiguration(configHolder.getDefaultConfigurationBuilder(), "default");
                for (Map.Entry<String, ConfigurationBuilder> entry : configHolder.getNamedConfigurationBuilders().entrySet()) {
                    verifyTransactionConfiguration(entry.getValue(), entry.getKey());
                }
                return new DefaultCacheManager(configHolder, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new DefaultCacheManager();
    }

    /**
     * Verifies that if a configuration has transactions enabled that it only uses the lookup that uses the
     * JBossStandaloneJTAManager, which looks up the transaction manager used by Quarkus
     *
     * @param configurationBuilder the current configuration
     * @param cacheName the cache for the configuration
     */
    private void verifyTransactionConfiguration(ConfigurationBuilder configurationBuilder, String cacheName) {
        TransactionConfigurationBuilder transactionConfigurationBuilder = configurationBuilder.transaction();
        if (transactionConfigurationBuilder.transactionMode() != null
                && transactionConfigurationBuilder.transactionMode().isTransactional()) {
            AttributeSet attributes = transactionConfigurationBuilder.attributes();
            Attribute<TransactionManagerLookup> managerLookup = attributes
                    .attribute(TransactionConfiguration.TRANSACTION_MANAGER_LOOKUP);
            if (managerLookup.isModified() && !(managerLookup.get() instanceof JBossStandaloneJTAManagerLookup)) {
                throw new CacheConfigurationException(
                        "Only JBossStandaloneJTAManagerLookup transaction manager lookup is supported. Cache " + cacheName
                                + " is misconfigured!");
            }
            managerLookup.set(new JBossStandaloneJTAManagerLookup());
        }
    }
}
