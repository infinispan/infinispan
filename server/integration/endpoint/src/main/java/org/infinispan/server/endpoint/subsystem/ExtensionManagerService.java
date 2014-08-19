package org.infinispan.server.endpoint.subsystem;

import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.KeyValueFilterFactory;
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
    private final Map<String, KeyValueFilterFactory> filterFactories = new HashMap<>();
    private final Map<String, ConverterFactory> converterFactories = new HashMap<>();

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
            for (Map.Entry<String, KeyValueFilterFactory> entry : filterFactories.entrySet())
                server.addKeyValueFilterFactory(entry.getKey(), entry.getValue());
        }

        synchronized (converterFactories) {
            for (Map.Entry<String, ConverterFactory> entry : converterFactories.entrySet())
                server.addConverterFactory(entry.getKey(), entry.getValue());
        }
    }

    public void removeHotRodServer(HotRodServer server) {
        synchronized (servers) {
            servers.remove(server);
        }

        synchronized (filterFactories) {
            for (String name : filterFactories.keySet())
                server.removeKeyValueFilterFactory(name);
        }

        synchronized (converterFactories) {
            for (String name : converterFactories.keySet())
                server.removeConverterFactory(name);
        }
    }

    public void addKeyValueFilterFactory(String name, KeyValueFilterFactory factory) {
        synchronized (filterFactories) {
            filterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addKeyValueFilterFactory(name, factory);
        }
    }

    public void removeKeyValueFilterFactory(String name) {
        synchronized (filterFactories) {
            filterFactories.remove(name);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.removeKeyValueFilterFactory(name);
        }
    }

    public void addConverterFactory(String name, ConverterFactory factory) {
        synchronized (converterFactories) {
            converterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addConverterFactory(name, factory);
        }
    }

    public void removeConverterFactory(String name) {
        synchronized (converterFactories) {
            converterFactories.remove(name);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.removeConverterFactory(name);
        }
    }

    @Override
    public ExtensionManagerService getValue() {
        return this;
    }

}
