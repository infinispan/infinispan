package org.infinispan.counter.impl.function;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The adding function to update the {@link CounterValue}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_ADD)
public final class AddFunction<K extends CounterKey> extends BaseFunction<K, CounterValue> {

   private static final Log log = LogFactory.getLog(AddFunction.class, Log.class);

   @ProtoField(number = 1, defaultValue = "-1")
   final long delta;

   @ProtoFactory
   public AddFunction(long delta) {
      this.delta = delta;
   }

   @Override
   CounterValue apply(ReadWriteEntryView<K, CounterValue> entry, CounterConfigurationMetaParam metadata) {
      return FunctionHelper.add(entry, entry.get(), metadata, delta);
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
