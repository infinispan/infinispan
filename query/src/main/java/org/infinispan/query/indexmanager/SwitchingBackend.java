package org.infinispan.query.indexmanager;

/**
 * Defines the strategy contract to be plugging into an InfinispanBackendQueueProcessor
 * 
 * @see InfinispanBackendQueueProcessor
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
public interface SwitchingBackend {

   void initialize();

   IndexingBackend getCurrentIndexingBackend();

   void shutdown();

}
