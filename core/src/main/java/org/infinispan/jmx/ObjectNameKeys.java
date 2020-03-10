package org.infinispan.jmx;

/**
 * @author anistor@redhat.com
 * @since 10.0
 */
public interface ObjectNameKeys {

   String NAME = "name";

   String TYPE = "type";   // Cache, CacheManager, Query, RemoteQuery, Server, etc.

   String COMPONENT = "component";

   String MANAGER = "manager";
}
