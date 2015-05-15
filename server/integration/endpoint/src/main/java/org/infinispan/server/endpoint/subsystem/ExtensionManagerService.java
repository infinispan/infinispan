package org.infinispan.server.endpoint.subsystem;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public class ExtensionManagerService implements Service<ExtensionManagerService> {

    private final List<HotRodServer> servers = new ArrayList<>();
    private final Map<String, CacheEventFilterFactory> filterFactories = new HashMap<>();
    private final Map<String, CacheEventConverterFactory> converterFactories = new HashMap<>();
    private final Map<String, CacheEventFilterConverterFactory> filterConverterFactories = new HashMap<>();
    private final Map<String, KeyValueFilterConverterFactory> keyValueFilterConverterFactories = new HashMap<>();

    @Override
    public void start(StartContext context) throws StartException {
        ROOT_LOGGER.debugf("Started server extension manager");
    }

    @Override
    public void stop(StopContext context) {
        ROOT_LOGGER.debugf("Stopped server extension manager");
    }

    public void addHotRodServer(HotRodServer server) {
        synchronized (servers) {
            servers.add(server);
        }

        synchronized (filterFactories) {
            for (Map.Entry<String, CacheEventFilterFactory> entry : filterFactories.entrySet())
                server.addCacheEventFilterFactory(entry.getKey(), entry.getValue());
        }

        synchronized (converterFactories) {
            for (Map.Entry<String, CacheEventConverterFactory> entry : converterFactories.entrySet())
                server.addCacheEventConverterFactory(entry.getKey(), entry.getValue());
        }

        synchronized (filterConverterFactories) {
            for (Map.Entry<String, CacheEventFilterConverterFactory> entry : filterConverterFactories.entrySet())
                server.addCacheEventFilterConverterFactory(entry.getKey(), entry.getValue());
        }

        synchronized (keyValueFilterConverterFactories) {
            for (Map.Entry<String, KeyValueFilterConverterFactory> entry : keyValueFilterConverterFactories.entrySet())
                server.addKeyValueFilterConverterFactory(entry.getKey(), entry.getValue());
        }
    }

    public void removeHotRodServer(HotRodServer server) {
        synchronized (servers) {
            servers.remove(server);
        }

        synchronized (filterFactories) {
            for (String name : filterFactories.keySet())
                server.removeCacheEventFilterFactory(name);
        }

        synchronized (converterFactories) {
            for (String name : converterFactories.keySet())
                server.removeCacheEventConverterFactory(name);
        }

        synchronized (filterConverterFactories) {
            for (String name : filterConverterFactories.keySet())
                server.removeCacheEventFilterConverterFactory(name);
        }

        synchronized (keyValueFilterConverterFactories) {
            for (String name : keyValueFilterConverterFactories.keySet())
                server.removeKeyValueFilterConverterFactory(name);
        }
    }

    public void addFilterFactory(String name, CacheEventFilterFactory factory) {
        synchronized (filterFactories) {
            filterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addCacheEventFilterFactory(name, factory);
        }
    }

    public void removeFilterFactory(String name) {
        synchronized (filterFactories) {
            filterFactories.remove(name);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.removeCacheEventFilterFactory(name);
        }
    }

    public void addConverterFactory(String name, CacheEventConverterFactory factory) {
        synchronized (converterFactories) {
            converterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addCacheEventConverterFactory(name, factory);
        }
    }

    public void removeConverterFactory(String name) {
        synchronized (converterFactories) {
            converterFactories.remove(name);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.removeCacheEventConverterFactory(name);
        }
    }

    public void addFilterConverterFactory(String name, CacheEventFilterConverterFactory factory) {
        synchronized (filterConverterFactories) {
            filterConverterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addCacheEventFilterConverterFactory(name, factory);
        }
    }

    public void removeFilterConverterFactory(String name) {
        synchronized (filterConverterFactories) {
            filterConverterFactories.remove(name);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.removeCacheEventFilterConverterFactory(name);
        }
    }

    public void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory) {
        synchronized (keyValueFilterConverterFactories) {
           keyValueFilterConverterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addKeyValueFilterConverterFactory(name, factory);
        }
    }

   public void removeKeyValueFilterConverterFactory(String name) {
        synchronized (keyValueFilterConverterFactories) {
           keyValueFilterConverterFactories.remove(name);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.removeKeyValueFilterConverterFactory(name);
        }
   }

    public void setMarshaller(Marshaller marshaller) {
       synchronized (servers) {
          for (HotRodServer server : servers)
             server.setMarshaller(marshaller);
       }
    }

    @Override
    public ExtensionManagerService getValue() {
        return this;
    }

}
