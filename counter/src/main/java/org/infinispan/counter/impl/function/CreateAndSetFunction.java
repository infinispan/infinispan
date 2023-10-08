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

@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_CREATE_AND_SET)
public class CreateAndSetFunction<K extends CounterKey> extends BaseCreateFunction<K, Object> {

   @ProtoField(2)
   final long value;

   @ProtoFactory
   public CreateAndSetFunction(CounterConfiguration configuration, long value) {
      super(configuration);
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue, CounterConfigurationMetaParam metadata) {
      return FunctionHelper.set(entryView, currentValue, metadata, value);
   }
}
