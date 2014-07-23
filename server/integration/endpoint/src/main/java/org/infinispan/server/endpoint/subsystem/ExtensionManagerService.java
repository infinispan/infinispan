package org.infinispan.server.endpoint.subsystem;

import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.KeyValueFilterFactory;
import org.infinispan.server.core.ProtocolServer;
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
            for (Map.Entry<String, KeyValueFilterFactory> entry : filterFactories.entrySet()) {
                server.addKeyValueFilterFactory(entry.getKey(), entry.getValue());
            }
        }

        synchronized (converterFactories) {
            for (Map.Entry<String, ConverterFactory> entry : converterFactories.entrySet()) {
                server.addConverterFactory(entry.getKey(), entry.getValue());
            }
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

    public void addConverterFactory(String name, ConverterFactory factory) {
        synchronized (converterFactories) {
            converterFactories.put(name, factory);
        }

        synchronized (servers) {
            for (HotRodServer server : servers)
                server.addConverterFactory(name, factory);
        }
    }

    @Override
    public ExtensionManagerService getValue() {
        return this;
    }

}
