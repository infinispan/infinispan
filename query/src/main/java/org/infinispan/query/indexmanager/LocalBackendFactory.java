package org.infinispan.query.indexmanager;

/**
 * Used to postpone creation of Local only IndexingBackend instances.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2014 Red Hat Inc.
 * @since 7.0
 */
interface LocalBackendFactory {

   IndexingBackend createLocalIndexingBackend();

}
