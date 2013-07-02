package org.infinispan.query.clustered;

import java.util.UUID;

import org.apache.lucene.search.FieldDoc;

/**
 * ClusteredFIeldDoc.
 * 
 * A FieldDoc with UUID of node who has the doc.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public final class ClusteredFieldDoc extends FieldDoc implements ClusteredDoc {

   /** The serialVersionUID */
   private static final long serialVersionUID = 1834188214178689282L;

   private final UUID nodeUuid;

   private final int index;

   public ClusteredFieldDoc(FieldDoc scoreDoc, UUID nodeUuid, int index) {
      super(scoreDoc.doc, scoreDoc.score, scoreDoc.fields);
      this.nodeUuid = nodeUuid;
      this.index = index;
   }

   @Override
   public UUID getNodeUuid() {
      return nodeUuid;
   }

   @Override
   public int getIndex(){
      return index;
   }

}