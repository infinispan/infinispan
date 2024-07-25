package org.infinispan.query.remote.impl.indexing.infinispan;

import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

public class IndexingMetadataHolder {

   private IndexingMetadata indexingMetadata;

   public IndexingMetadata getIndexingMetadata() {
      return indexingMetadata;
   }

   public void setIndexingMetadata(IndexingMetadata indexingMetadata) {
      this.indexingMetadata = indexingMetadata;
   }
}
