package org.infinispan.counter.impl.listener;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.filter.KeyFilter;

/**
 * A {@link KeyFilter} to produce events for all counters in the cache.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class CounterKeyFilter implements KeyFilter<Object> {

   public static final AdvancedExternalizer<CounterKeyFilter> EXTERNALIZER = new Externalizer();
   private static final CounterKeyFilter INSTANCE = new CounterKeyFilter();

   private CounterKeyFilter() {
   }

   public static CounterKeyFilter getInstance() {
      return INSTANCE;
   }

   @Override
   public String toString() {
      return "CounterKeyFilter()";
   }

   @Override
   public boolean accept(Object key) {
      return key instanceof CounterKey;
   }

   private static class Externalizer implements AdvancedExternalizer<CounterKeyFilter> {

      @Override
      public Set<Class<? extends CounterKeyFilter>> getTypeClasses() {
         return Collections.singleton(CounterKeyFilter.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CONVERTER_AND_FILTER;
      }

      @Override
      public void writeObject(ObjectOutput output, CounterKeyFilter object) throws IOException {

      }

      @Override
      public CounterKeyFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }
}
