package org.infinispan.query.indexmanager;

/**
 * Alternative implementation to the ClusteredSwitchingBackend, meant
 * to be used for non-clustered caches: much simpler as we have no
 * states nor transitions to manage.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
public class LocalOnlyBackend implements SwitchingBackend {

   private IndexingBackend localIndexingBackend;
   private LocalBackendFactory factory;

   public LocalOnlyBackend(LocalBackendFactory factory) {
      this.factory = factory;
   }

   @Override
   public void initialize() {
      this.localIndexingBackend = factory.createLocalIndexingBackend();
      this.factory = null;
   }

   @Override
   public IndexingBackend getCurrentIndexingBackend() {
      return localIndexingBackend;
   }

   @Override
   public void shutdown() {
      localIndexingBackend.flushAndClose(null);
   }

}
