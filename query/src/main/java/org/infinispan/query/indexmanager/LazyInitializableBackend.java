package org.infinispan.query.indexmanager;

/**
 * Some SwitchingBackend implementations need to expose additional transition methods.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
interface LazyInitializableBackend extends SwitchingBackend {

   void lazyInitialize();

   boolean attemptUpgrade(IndexingBackend expectedBackend);

}
