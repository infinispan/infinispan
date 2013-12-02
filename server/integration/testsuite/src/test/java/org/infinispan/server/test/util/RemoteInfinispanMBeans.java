package org.infinispan.server.test.util;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RemoteInfinispanServers;
import org.infinispan.arquillian.model.RemoteInfinispanCache;
import org.infinispan.arquillian.model.RemoteInfinispanCacheManager;

/**
 * A tuple-style object holder containing references to Remote interfaces for JMX statistics access.
 * 
 * @author Michal Linhard (mlinhard@redhat.com)
 * 
 */
public class RemoteInfinispanMBeans {
    public String serverName;
    public String cacheName;
    public String managerName;
    public RemoteInfinispanServer server;
    public RemoteInfinispanCache cache;
    public RemoteInfinispanCacheManager manager;

    public static RemoteInfinispanMBeans create(RemoteInfinispanServers servers, String serverName, String cacheName,
        String managerName) {
        RemoteInfinispanMBeans r = new RemoteInfinispanMBeans();
        r.serverName = serverName;
        r.cacheName = cacheName;
        r.managerName = managerName;
        r.server = servers.getServer(serverName);
        r.manager = r.server.getCacheManager(managerName);
        r.cache = r.manager.getCache(cacheName);
        return r;
    }
}
