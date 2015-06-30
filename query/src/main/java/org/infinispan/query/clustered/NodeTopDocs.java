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

   public NodeTopDocs(TopDocs topDocs, Object[] keys) {
      this.topDocs = topDocs;
      this.keys = keys;
   }

   public NodeTopDocs(TopDocs topDocs) {
      this.topDocs = topDocs;
      this.keys = null;
   }
}
