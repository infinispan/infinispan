package org.infinispan.counter.impl.function;

import java.io.Serializable;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * The function to set the {@link CounterValue}.
 *
 * @author Dipanshu Gupta
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_SET)
public final class SetFunction<K extends CounterKey> extends BaseFunction<K, Object> implements Serializable {
   private static final Log log = LogFactory.getLog(SetFunction.class, Log.class);

   @ProtoField(1)
   final long value;

   @ProtoFactory
   public SetFunction(long value) {
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entry, CounterConfigurationMetaParam metadata) {
      return FunctionHelper.set(entry, entry.get(), metadata, value);
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
