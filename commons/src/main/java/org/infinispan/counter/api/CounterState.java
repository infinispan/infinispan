package org.infinispan.counter.api;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;

/**
 * The possible states for a counter value.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum CounterState {
   /**
    * The counter value is valid.
    */
   VALID,
   /**
    * The counter value has reached its min threshold, i.e. no thresholds has been reached.
    */
   LOWER_BOUND_REACHED,
   /**
    * The counter value has reached its max threshold.
    */
   UPPER_BOUND_REACHED;

   private static final CounterState[] CACHED_VALUES = CounterState.values();
   public static final AdvancedExternalizer<CounterState> EXTERNALIZER = new Externalizer();

   public static CounterState valueOf(int index) {
      return CACHED_VALUES[index];
   }

   private static class Externalizer implements AdvancedExternalizer<CounterState> {

      @Override
      public Set<Class<? extends CounterState>> getTypeClasses() {
         return Collections.singleton(CounterState.class);
      }

      @Override
      public Integer getId() {
         return Ids.COUNTER_STATE;
      }

      @Override
      public void writeObject(UserObjectOutput output, CounterState object) throws IOException {
         MarshallUtil.marshallEnum(object, output);
      }

      @Override
      public CounterState readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return MarshallUtil.unmarshallEnum(input, CounterState::valueOf);
      }
   }

}
