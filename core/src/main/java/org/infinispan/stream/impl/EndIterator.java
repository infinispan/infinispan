package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;

/**
 * Singleton object with no state that is used to signal that an iterator has reached the end. This is useful for
 * serialization/deserialization so you don't have to query the total number of elements.
 * @author wburns
 * @since 9.0
 */
public class EndIterator {
   private EndIterator() { }

   private static EndIterator INSTANCE = new EndIterator();

   public static EndIterator getInstance() {
      return INSTANCE;
   }

   public static class EndIteratorExternalizer extends AbstractExternalizer<EndIterator> {

      @Override
      public Integer getId() {
         return Ids.END_ITERATOR;
      }

      @Override
      public Set<Class<? extends EndIterator>> getTypeClasses() {
         return Collections.singleton(EndIterator.class);
      }

      @Override
      public void writeObject(ObjectOutput output, EndIterator object) throws IOException {

      }

      @Override
      public EndIterator readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return EndIterator.getInstance();
      }
   }
}
