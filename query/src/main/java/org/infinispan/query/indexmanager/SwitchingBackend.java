package org.infinispan.query.indexmanager;

/**
 * Defines the strategy contract to be plugging into an InfinispanBackendQueueProcessor
 *
 * @see InfinispanBackendQueueProcessor
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
interface SwitchingBackend {

   void initialize();

   IndexingBackend getCurrentIndexingBackend();

   void refresh();

   void shutdown();
}
