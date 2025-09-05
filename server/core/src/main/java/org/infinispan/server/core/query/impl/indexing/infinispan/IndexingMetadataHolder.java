package org.infinispan.server.core.query.impl.indexing.infinispan;

import org.infinispan.server.core.query.impl.indexing.IndexingMetadata;

public class IndexingMetadataHolder {

   private IndexingMetadata indexingMetadata;

   public IndexingMetadata getIndexingMetadata() {
      return indexingMetadata;
   }

   public void setIndexingMetadata(IndexingMetadata indexingMetadata) {
      this.indexingMetadata = indexingMetadata;
   }
}
