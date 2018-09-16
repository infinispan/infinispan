package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.search.TopDocs;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.remoting.transport.Address;

/**
 * A TopDocs with an array with keys of each result.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
public final class NodeTopDocs {

   public final Address address;
   public final TopDocs topDocs;
   public final Object[] keys;
   public final Object[] projections;

   public NodeTopDocs(Address address, TopDocs topDocs, Object[] keys, Object[] projections) {
      this.address = address;
      this.topDocs = topDocs;
      this.keys = keys;
      this.projections = projections;
   }

   public NodeTopDocs(Address address, TopDocs topDocs) {
      this.address = address;
      this.topDocs = topDocs;
      this.keys = null;
      this.projections = null;
   }

   public static final class Externalizer extends AbstractExternalizer<NodeTopDocs> {

      @Override
      public Set<Class<? extends NodeTopDocs>> getTypeClasses() {
         return Collections.singleton(NodeTopDocs.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CLUSTERED_QUERY_TOPDOCS;
      }

      @Override
      public NodeTopDocs readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Address address = (Address) input.readObject();
         int keysNumber = UnsignedNumeric.readUnsignedInt(input);
         Object[] keys = new Object[keysNumber];
         for (int i = 0; i < keysNumber; i++) {
            keys[i] = input.readObject();
         }
         int projectionsNumber = UnsignedNumeric.readUnsignedInt(input);
         Object[] projections = new Object[projectionsNumber];
         for (int i = 0; i < projectionsNumber; i++) {
            projections[i] = input.readObject();
         }
         TopDocs innerTopDocs = (TopDocs) input.readObject();
         return new NodeTopDocs(address, innerTopDocs, keys, projections);
      }

      @Override
      public void writeObject(ObjectOutput output, NodeTopDocs topDocs) throws IOException {
         output.writeObject(topDocs.address);
         Object[] keys = topDocs.keys;
         int size = keys == null ? 0 : keys.length;
         UnsignedNumeric.writeUnsignedInt(output, size);
         for (int i = 0; i < size; i++) {
            output.writeObject(keys[i]);
         }
         Object[] projections = topDocs.projections;
         int projectionSize = projections == null ? 0 : projections.length;
         UnsignedNumeric.writeUnsignedInt(output, projectionSize);
         for (int i = 0; i < projectionSize; i++) {
            output.writeObject(projections[i]);
         }
         output.writeObject(topDocs.topDocs);
      }
   }
}
