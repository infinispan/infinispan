package org.infinispan.query.test;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class CustomKey3 {

   private final String str;

   @ProtoFactory
   public CustomKey3(String str) {
      this.str = str;
   }

   @ProtoField(number = 1)
   public String getStr() {
      return str;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomKey3 that = (CustomKey3) o;
      return str != null ? str.equals(that.str) : that.str == null;
   }

   @Override
   public int hashCode() {
      return str != null ? str.hashCode() : 0;
   }
}
