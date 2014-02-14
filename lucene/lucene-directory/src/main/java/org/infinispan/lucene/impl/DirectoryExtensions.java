package org.infinispan.lucene.impl;

import org.infinispan.Cache;

/**
 * Some additional methods we add to our Directory implementations,
 * mostly for reporting and testing reasons.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2013 Red Hat Inc.
 */
public interface DirectoryExtensions {

   String getIndexName();

   //Was part of the Directory contract for Lucene 2.9.x
   void renameFile(final String from, final String to);

   int getChunkSize();

   Cache getMetadataCache();

   Cache getDataCache();

}
