package org.infinispan.client.hotrod.event;

/**
 * Event received when the registered listener fails over to a different node.
 * Receiving this event indicates that a failure happened in the node where
 * the listener was registered in and another server has been selected for
 * installing the listener in. As a result of this failover, some events might
 * have been missed, hence, this event can be used to clear locally cached
 * data. After this failover event is received, the entire cache contents will
 * be iterated over and the client receives events on these contents, which
 * can be used to rebuild any locally built cache.
 *
 * @author Galder Zamarreño
 */
public interface ClientCacheFailoverEvent extends ClientEvent {

}
