package org.infinispan.query.test;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "transactionIndex")
public class Transaction implements Serializable {

   private final int size;

   private final String script;

   @ProtoFactory
   public Transaction(int size, String script) {
      this.size = size;
      this.script = script;
   }

   @Field(analyze = Analyze.NO)
   @ProtoField(number = 1, defaultValue = "0")
   public int getSize() {
      return size;
   }

   @Field
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
