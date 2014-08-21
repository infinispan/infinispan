package org.infinispan.query.indexmanager;

/**
 * Used to postpone creation of Local only IndexingBackend instances.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
interface LocalBackendFactory {

   IndexingBackend createLocalIndexingBackend();

}
