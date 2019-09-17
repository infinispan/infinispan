package org.infinispan.query.test;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "commonIndex")
public class Transaction {

   @Field(analyze = Analyze.NO)
   private final int size;

   @Field
   private final String script;

   @ProtoFactory
   public Transaction(int size, String script) {
      this.size = size;
      this.script = script;
   }

   @ProtoField(number = 1, defaultValue = "0")
   public int getSize() {
      return size;
   }

   @ProtoField(number = 2)
   public String getScript() {
      return script;
   }

   @Override
   public String toString() {
      return "Transaction{" +
            "size=" + size +
            ", script='" + script + '\'' +
            '}';
   }
}
