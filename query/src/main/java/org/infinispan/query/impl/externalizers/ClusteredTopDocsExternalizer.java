package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.TopDocs;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.clustered.NodeTopDocs;

public class ClusteredTopDocsExternalizer extends AbstractExternalizer<NodeTopDocs> {

   @Override
   public Set<Class<? extends NodeTopDocs>> getTypeClasses() {
      return Collections.singleton(NodeTopDocs.class);
   }

   @Override
   public NodeTopDocs readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final int keysNumber = UnsignedNumeric.readUnsignedInt(input);
      final Object[] keys = new Object[keysNumber];
      for (int i = 0; i < keysNumber; i++) {
         keys[i] = input.readObject();
      }
      final int projectionsNumber = UnsignedNumeric.readUnsignedInt(input);
      final Object[] projections = new Object[projectionsNumber];
      for (int i = 0; i < projectionsNumber; i++) {
         projections[i] = input.readObject();
      }
      final TopDocs innerTopDocs = (TopDocs) input.readObject();
      return new NodeTopDocs(innerTopDocs, keys, projections);
   }

   @Override
   public void writeObject(final ObjectOutput output, final NodeTopDocs topDocs) throws IOException {
      final Object[] keys = topDocs.keys;
      int size = keys == null ? 0 : keys.length;
      UnsignedNumeric.writeUnsignedInt(output, size);
      for (int i = 0; i < size; i++) {
         output.writeObject(keys[i]);
      }
      final Object[] projections = topDocs.projections;
      int projectionSize = projections == null ? 0 : projections.length;
      UnsignedNumeric.writeUnsignedInt(output, projectionSize);
      for (int i = 0; i < projectionSize; i++) {
         output.writeObject(projections[i]);
      }
      output.writeObject(topDocs.topDocs);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.CLUSTERED_QUERY_TOPDOCS;
   }
}
