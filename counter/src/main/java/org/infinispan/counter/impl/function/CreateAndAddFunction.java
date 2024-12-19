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
 * The adding function to update the {@link CounterValue}.
 * <p>
 * If the {@link CounterValue} doesn't exist, it is created. This is a difference between {@link AddFunction} and this
 * class.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_CREATE_AND_ADD)
public class CreateAndAddFunction<K extends CounterKey> extends BaseCreateFunction<K, CounterValue> {

   @ProtoField(number = 2, defaultValue = "-1")
   final long delta;

   @ProtoFactory
   public CreateAndAddFunction(CounterConfiguration configuration, long delta) {
      super(configuration);
      this.delta = delta;
   }

   @Override
   CounterValue apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue,
         CounterConfigurationMetaParam metadata) {
      return FunctionHelper.add(entryView, currentValue, metadata, delta);
   }
}
