package org.infinispan.server.infinispan.spi.service;

import org.jboss.msc.service.ServiceName;

/**
 * Enumeration of service name factories for services associated with a counter.
 *
 * @author Vladimir Blagojevic
 */
public class CounterServiceName {

    public static ServiceName getServiceName(String container, String counterName) {
        return CacheContainerServiceName.CACHE_CONTAINER.getServiceName(container).append(counterName);
    }

}
