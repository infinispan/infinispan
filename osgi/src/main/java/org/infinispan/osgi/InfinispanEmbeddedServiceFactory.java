package org.infinispan.osgi;

import java.io.InputStream;
import java.net.URL;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.util.FileLookup;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;

public class InfinispanEmbeddedServiceFactory implements ManagedServiceFactory {
   /**
    * Configuration property used to indicate the cache configuration to use.
    */
    private static final String PROP_CONFIG = "config";

    /**
     * Configuration property used to identify the service created (e.g. for service filters).
     */
    private static final String PROP_INSTANCE_ID = "instanceId";

    private BundleContext bundleContext;
    private Map<String, DefaultCacheManager> cacheManagers = new HashMap<String, DefaultCacheManager>();
    private Map<String, ServiceRegistration<EmbeddedCacheManager>> managedRegistrations = new HashMap<String, ServiceRegistration<EmbeddedCacheManager>>();

    @Override
    public String getName() {
        return "Infinispan Embedded Managed Service Factory";
    }

    @Override
    public synchronized void updated(String pid, @SuppressWarnings("rawtypes") Dictionary properties) throws ConfigurationException {
        String config = (String) properties.get(PROP_CONFIG);
        if (config == null) {
            throw new ConfigurationException(PROP_CONFIG, "Property must be set");
        }

        String instanceId = (String) properties.get(PROP_INSTANCE_ID);
        if (instanceId == null) {
            throw new ConfigurationException(PROP_INSTANCE_ID, "Property must be set");
        }

        try {
            URL configURL = new FileLookup().lookupFileLocation(config, Thread.currentThread().getContextClassLoader());
            if (configURL == null) {
                throw new ConfigurationException(PROP_CONFIG, String.format("Failed to find the specified config '%s'.", config));
            }

            /* Unregister and destroy the old object. */
            deleted(pid);

            InputStream configStream = configURL.openStream();
            DefaultCacheManager cacheManager = new DefaultCacheManager(configStream);

            cacheManager.start();

            Hashtable<String, String> ht = new Hashtable<String, String>();
            ht.put(PROP_INSTANCE_ID, instanceId);
            ht.put(PROP_CONFIG, config);

            ServiceRegistration<EmbeddedCacheManager> serviceRegistration = bundleContext.registerService(EmbeddedCacheManager.class, cacheManager, ht);

            managedRegistrations.put(pid, serviceRegistration);
            cacheManagers.put(pid, cacheManager);
        } catch (Exception e) {
            throw new ConfigurationException(null, "Cannot start the CacheManager", e);
        }
    }

    @Override
    public synchronized void deleted(String pid) {
        ServiceRegistration<EmbeddedCacheManager> serviceRegistration = managedRegistrations.remove(pid);
        if (serviceRegistration != null) {
            try {
                serviceRegistration.unregister();
            } catch (Exception e) {
            }
        }

        DefaultCacheManager cacheManager = cacheManagers.remove(pid);
        if (cacheManager != null) {
            try {
                cacheManager.stop();
            } catch (Exception e) {
            }
        }
    }

    public synchronized void destroy() {
        /* Create a copy of the keyset before iterating over it to avoid ConcurrentModificationExceptions. */
        Set<String> keys = new HashSet<String>(cacheManagers.keySet());
        for (String cacheManager: keys) {
            deleted(cacheManager);
        }
    }

    public BundleContext getBundleContext() {
        return bundleContext;
    }

    public void setBundleContext(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }
}
