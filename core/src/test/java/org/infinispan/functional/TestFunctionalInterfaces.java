package org.infinispan.functional;

import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class TestFunctionalInterfaces {

   public static final class SetConstantOnReadWrite<K> implements Function<ReadWriteEntryView<K, String>, Void> {
      @ProtoField(1)
      final String constant;

      @ProtoFactory
      public SetConstantOnReadWrite(String constant) {
         this.constant = constant;
      }

      @Override
      public Void apply(ReadWriteEntryView<K, String> rw) {
         rw.set(constant);
         return null;
      }
   }

   public static final class SetConstantOnWriteOnly<K> implements Consumer<WriteEntryView<K, String>> {
      @ProtoField(1)
      final String constant;

      @ProtoFactory
      public SetConstantOnWriteOnly(String constant) {
         this.constant = constant;
      }

      @Override
      public void accept(WriteEntryView<K, String> wo) {
         wo.set(constant);
      }
   }
}
