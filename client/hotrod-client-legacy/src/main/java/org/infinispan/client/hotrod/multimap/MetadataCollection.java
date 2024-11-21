package org.infinispan.client.hotrod.multimap;

import java.util.Collection;

import org.infinispan.client.hotrod.Metadata;
import org.infinispan.client.hotrod.Versioned;

/**
 * Metadata and collection, used for Multimap
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public interface MetadataCollection<V> extends Versioned, Metadata {

   /**
    * Collection of values with metadata
    * @return the collection
    */
   Collection<V> getCollection();
}
