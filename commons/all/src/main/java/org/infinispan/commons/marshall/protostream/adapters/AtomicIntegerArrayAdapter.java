package org.infinispan.commons.marshall.protostream.adapters;

import java.util.concurrent.atomic.AtomicIntegerArray;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.ATOMIC_INTEGER_ARRAY)
@ProtoAdapter(AtomicIntegerArray.class)
public class AtomicIntegerArrayAdapter {

   @ProtoFactory
   public AtomicIntegerArray create(int[] elements) {
      return new AtomicIntegerArray(elements);
   }

   @ProtoField(1)
   int[] getElements(AtomicIntegerArray array) {
      int[] elements = new int[array.length()];
      for (int i = 0; i < array.length(); i++) {
         elements[i] = array.get(i);
      }
      return elements;
   }
}
