package org.infinispan.query.impl.protostream.adapters;

import java.util.Arrays;

import org.apache.lucene.util.BytesRef;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoAdapter(BytesRef.class)
@ProtoTypeId(ProtoStreamTypeIds.LUCENE_BYTES_REF)
public class LuceneBytesRefAdapter {

   @ProtoFactory
   static BytesRef protoFactory(byte[] bytes) {
      return new BytesRef(bytes);
   }

   @ProtoField(1)
   byte[] getBytes(BytesRef ref) {
      return Arrays.copyOfRange(ref.bytes, ref.offset, ref.length);
   }
}
