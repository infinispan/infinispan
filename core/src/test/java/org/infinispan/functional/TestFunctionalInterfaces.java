package org.infinispan.functional;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

public class TestFunctionalInterfaces {

   @SerializeWith(value = SetConstantOnReadWrite.Externalizer0.class)
   public static final class SetConstantOnReadWrite<K>
         implements Function<ReadWriteEntryView<K, String>, Void> {
      final String constant;

      public SetConstantOnReadWrite(String constant) {
         this.constant = constant;
      }

      @Override
      public Void apply(ReadWriteEntryView<K, String> rw) {
         rw.set(constant);
         return null;
      }

      public static final class Externalizer0
            implements Externalizer<SetConstantOnReadWrite<?>> {
         @Override
         public void writeObject(ObjectOutput output, SetConstantOnReadWrite<?> object)
               throws IOException {
            output.writeUTF(object.constant);
         }

         @Override
         public SetConstantOnReadWrite<?> readObject(ObjectInput input)
               throws IOException, ClassNotFoundException {
            String constant = input.readUTF();
            return new SetConstantOnReadWrite<>(constant);
         }
      }
   }

   @SerializeWith(value = SetConstantOnWriteOnly.Externalizer0.class)
   public static final class SetConstantOnWriteOnly<K> implements Consumer<WriteEntryView<K, String>> {
      final String constant;

      public SetConstantOnWriteOnly(String constant) {
         this.constant = constant;
      }

      @Override
      public void accept(WriteEntryView<K, String> wo) {
         wo.set(constant);
      }


      public static final class Externalizer0 implements Externalizer<SetConstantOnWriteOnly> {
         @Override
         public void writeObject(ObjectOutput output, SetConstantOnWriteOnly object)
               throws IOException {
            output.writeUTF(object.constant);
         }

         @Override
         public SetConstantOnWriteOnly readObject(ObjectInput input)
               throws IOException, ClassNotFoundException {
            String constant = input.readUTF();
            return new SetConstantOnWriteOnly(constant);
         }
      }
   }

}
