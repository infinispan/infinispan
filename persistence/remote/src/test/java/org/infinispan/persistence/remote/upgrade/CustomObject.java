package org.infinispan.persistence.remote.upgrade;

import java.io.Serializable;
import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class CustomObject implements Serializable {

   @ProtoFactory
   public CustomObject(String text, Integer number) {
      this.text = text;
      this.number = number;
   }

   @ProtoField(number = 1)
   String text;

   @ProtoField(number = 2)
   Integer number;

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomObject that = (CustomObject) o;

      if (!Objects.equals(text, that.text)) return false;
      return Objects.equals(number, that.number);
   }

   @Override
   public int hashCode() {
      int result = text != null ? text.hashCode() : 0;
      result = 31 * result + (number != null ? number.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "CustomObject{" +
            "text='" + text + '\'' +
            ", number=" + number +
            '}';
   }
}
