/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infinispan.osgi;

import java.lang.Thread;
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
    synchronized public void updated(String pid, Dictionary properties) throws ConfigurationException {
        deleted(pid);

        String config = (String) properties.get("config");
        if (config == null) {
            throw new ConfigurationException("config", "Property must be set");
        }
        String instanceId = (String)properties.get(INSTANCE_ID);
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
    synchronized public void deleted(String pid) {
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

    synchronized public void destroy() {
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
