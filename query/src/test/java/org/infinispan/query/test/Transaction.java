package org.infinispan.query.test;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
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

   @Basic
   @ProtoField(number = 1, defaultValue = "0")
   public int getSize() {
      return size;
   }

   @Basic
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
