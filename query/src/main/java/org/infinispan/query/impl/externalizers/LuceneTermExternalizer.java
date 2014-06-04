package org.infinispan.query.impl.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

public class LuceneTermExternalizer extends AbstractExternalizer<Term> {

   @Override
   public Set<Class<? extends Term>> getTypeClasses() {
      return Util.<Class<? extends Term>>asSet(Term.class);
   }

   @Override
   public Term readObject(final ObjectInput input) throws IOException, ClassNotFoundException {
      final String fieldName = input.readUTF();
      final int payloadSize = UnsignedNumeric.readUnsignedInt(input);
      final byte[] readBuffer = new byte[payloadSize];
      input.readFully(readBuffer);
      return new Term(fieldName,new BytesRef(readBuffer));
   }

   @Override
   public void writeObject(final ObjectOutput output, final Term term) throws IOException {
      output.writeUTF(term.field());
      final BytesRef payload = term.bytes();
      UnsignedNumeric.writeUnsignedInt(output, payload.length);
      output.write(payload.bytes, payload.offset, payload.length);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_TERM;
   }

}
