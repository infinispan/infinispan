package org.infinispan.query.clustered;

import org.apache.lucene.search.TopDocs;

/**
 * NodeTopDocs.
 * <p>
 * A TopDocs with an array with keys of each result.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class NodeTopDocs {

   public final TopDocs topDocs;
   public final Object[] keys;
   public final Object[] projections;

   public NodeTopDocs(TopDocs topDocs, Object[] keys, Object[] projections) {
      this.topDocs = topDocs;
      this.keys = keys;
      this.projections = projections;
   }

   public NodeTopDocs(TopDocs topDocs) {
      this.topDocs = topDocs;
      this.keys = null;
      this.projections = null;
   }
}
