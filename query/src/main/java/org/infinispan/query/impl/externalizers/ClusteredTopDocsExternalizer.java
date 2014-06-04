package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.search.TopDocs;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.query.clustered.ISPNEagerTopDocs;

public class ClusteredTopDocsExternalizer extends AbstractExternalizer<ISPNEagerTopDocs> {

   @Override
   public Set<Class<? extends ISPNEagerTopDocs>> getTypeClasses() {
      return Util.<Class<? extends ISPNEagerTopDocs>>asSet(ISPNEagerTopDocs.class);
   }

   @Override
   public ISPNEagerTopDocs readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final int keysNumber = UnsignedNumeric.readUnsignedInt(input);
      final Object[] keys = new Object[keysNumber];
      for (int i=0; i<keysNumber; i++) {
         keys[i] = input.readObject();
      }
      final TopDocs innerTopDocs = LuceneTopDocsExternalizer.readObjectStatic(input);
      return new ISPNEagerTopDocs(innerTopDocs, keys);
   }

   @Override
   public void writeObject(final ObjectOutput output, final ISPNEagerTopDocs topDocs) throws IOException {
      final Object[] keys = topDocs.keys;
      UnsignedNumeric.writeUnsignedInt(output, keys.length);
      for (Object o : keys) {
         output.writeObject(o);
      }
      LuceneTopDocsExternalizer.writeObjectStatic(output, topDocs);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.CLUSTERED_QUERY_TOPDOCS;
   }
}
