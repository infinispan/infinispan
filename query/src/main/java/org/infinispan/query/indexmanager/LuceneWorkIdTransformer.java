package org.infinispan.query.indexmanager;

import org.hibernate.search.backend.LuceneWork;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
interface LuceneWorkIdTransformer<T extends LuceneWork> {

   T cloneOverridingIdString(T lw, KeyTransformationHandler keyTransformationHandler);

}
