package org.infinispan.query.impl.externalizers;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;

public class LuceneBytesRefExternalizer extends AbstractExternalizer<BytesRef> {

   @Override
   public Integer getId() {
      return ExternalizerIds.LUCENE_BYTES_REF;
   }

   @Override
   public Set<Class<? extends BytesRef>> getTypeClasses() {
      return Collections.singleton(BytesRef.class);
   }

   @Override
   public void writeObject(ObjectOutput output, BytesRef object) throws IOException {
      UnsignedNumeric.writeUnsignedInt(output, object.length);
      output.write(object.bytes, object.offset, object.length);
   }

   @Override
   public BytesRef readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int length = UnsignedNumeric.readUnsignedInt(input);
      byte[] bytes = new byte[length];
      input.readFully(bytes, 0, length);
      return new BytesRef(bytes, 0, length);
   }
}
