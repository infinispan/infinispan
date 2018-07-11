package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSetsExternalization;

/**
 * Externalizer to be used for IntSet implementations
 * @author wburns
 * @since 9.0
 */
public class IntSetExternalizer extends AbstractExternalizer<IntSet> {

   @Override
   public Integer getId() {
      return Ids.INT_SET;
   }

   @Override
   public Set<Class<? extends IntSet>> getTypeClasses() {
      return IntSetsExternalization.getTypeClasses();
   }

   @Override
   public void writeObject(ObjectOutput output, IntSet intSet) throws IOException {
      IntSetsExternalization.writeTo(output, intSet);
   }

   @Override
   public IntSet readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return IntSetsExternalization.readFrom(input);
   }
}
