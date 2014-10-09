package org.infinispan.osgi;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedServiceFactory;

public class InfinispanServiceFactory implements ManagedServiceFactory {

    public static final String INSTANCE_ID = "instanceId";

    private BundleContext bundleContext;
    private Map<String, DefaultCacheManager> cacheManagers = new HashMap<String, DefaultCacheManager>();
    private Map<String, ServiceRegistration> managedRegistrations = new HashMap<String, ServiceRegistration>();

    @Override
    public String getName() {
        return "Infinispan Server Controller";
    }

    @Override
    public synchronized void updated(String pid, Dictionary properties) throws ConfigurationException {
        deleted(pid);

        String config = (String) properties.get("config");
        if (config == null) {
            throw new ConfigurationException("config", "Property must be set");
        }
        String instanceId = (String) properties.get(INSTANCE_ID);
        if (instanceId == null) {
            throw new ConfigurationException(INSTANCE_ID, "Property must be set");
        }

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(DefaultCacheManager.class.getClassLoader());

            DefaultCacheManager cacheManager = new DefaultCacheManager(config);

            cacheManager.start();
            cacheManagers.put(pid, cacheManager);

            Hashtable ht = new Hashtable();
            ht.put(Constants.SERVICE_PID, pid);
            ht.put(INSTANCE_ID, instanceId);

            ServiceRegistration serviceRegistration = bundleContext.registerService(EmbeddedCacheManager.class.getName(), cacheManager, ht);

            managedRegistrations.put(pid, serviceRegistration);
        } catch (Exception e) {
            throw new ConfigurationException(null, "Cannot start the CacheManager", e);
        } finally {
            Thread.currentThread().setContextClassLoader(classLoader);
        }
    }

    @Override
    public synchronized void deleted(String pid) {
        ServiceRegistration serviceRegistration = managedRegistrations.remove(pid);
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
        for (String cacheManager: cacheManagers.keySet()) {
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
