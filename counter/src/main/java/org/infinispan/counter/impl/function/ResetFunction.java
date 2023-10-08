package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Reset function that sets the counter's delta to it's initial delta.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_RESET)
public class ResetFunction<K extends CounterKey> extends BaseFunction<K, Void> {

   private static final Log log = LogFactory.getLog(ResetFunction.class, Log.class);

   private static final ResetFunction INSTANCE = new ResetFunction();

   private ResetFunction() {
   }

   @ProtoFactory
   public static <K extends CounterKey> ResetFunction<K> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   @Override
   Void apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterConfigurationMetaParam metadata) {
      entryView.set(newCounterValue(metadata.get()), metadata);
      return null;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public String toString() {
      return "ResetFunction{}";
   }
}
