package org.infinispan.counter.impl.function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The compare-and-swap function to update the {@link CounterValue}.
 * <p>
 * It has the same semantic as {@link CompareAndSwapFunction} but it creates the {@link CounterValue} if it doesn't
 * exist.
 *
 * @author Pedro Ruivo
 * @see CompareAndSwapFunction
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_CREATE_AND_CAS)
public class CreateAndCASFunction<K extends CounterKey> extends BaseCreateFunction<K, Object> {

   @ProtoField(2)
   final long expect;

   @ProtoField(3)
   final long value;

   @ProtoFactory
   public CreateAndCASFunction(CounterConfiguration configuration, long expect, long value) {
      super(configuration);
      this.expect = expect;
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue,
         CounterConfigurationMetaParam metadata) {
      return FunctionHelper.compareAndSwap(entryView, currentValue, metadata, expect, value);
   }
}
