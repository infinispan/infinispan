package org.infinispan.lucene.impl;

/**
 * @author gustavonalle
 * @since 7.0
 */
interface Operation {

   void apply(FileListCacheValue target);

}
