package org.infinispan.test.data;


import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoField;

public class Value {

   @ProtoField(number = 1)
   String name;

   @ProtoField(number = 2)
   String value;

   Value() {}

   public Value(String name, String value) {
      this.name = name;
      this.value = value;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Value value1 = (Value) o;
      return Objects.equals(name, value1.name) &&
            Objects.equals(value, value1.value);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, value);
   }

   @Override
   public String toString() {
      return "Value{" +
            "name='" + name + '\'' +
            ", value='" + value + '\'' +
            '}';
   }
}
