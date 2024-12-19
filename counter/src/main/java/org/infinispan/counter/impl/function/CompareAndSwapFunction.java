package org.infinispan.counter.impl.function;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The compare-and-swap function to update the {@link CounterValue}.
 * <p>
 * It returns the previous value and it is considered successful when the return value is the {@code expect}ed.
 * <p>
 * For a bounded counter (if the current value is equal to the {@code expect}ed), if the {@code value} is outside the
 * bounds, it returns {@link CounterState#LOWER_BOUND_REACHED} or {@link CounterState#UPPER_BOUND_REACHED} if the lower
 * bound or upper bound is violated.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_CAS)
public class CompareAndSwapFunction<K extends CounterKey> extends BaseFunction<K, Object> {

   private static final Log log = LogFactory.getLog(CompareAndSwapFunction.class, Log.class);

   @ProtoField(number = 1, defaultValue = "-1")
   final long expect;

   @ProtoField(number = 2, defaultValue = "-1")
   final long value;

   @ProtoFactory
   public CompareAndSwapFunction(long expect, long value) {
      this.expect = expect;
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterConfigurationMetaParam metadata) {
      return FunctionHelper.compareAndSwap(entryView, entryView.get(), metadata, expect, value);
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
