package org.infinispan.test.data;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Numerics {

   public Numerics() { }

   @ProtoFactory
   public Numerics(int keyColumn, long simpleLong, float simpleFloat, double simpleDouble, long largeInteger) {
      this.keyColumn = keyColumn;
      this.simpleLong = simpleLong;
      this.simpleFloat = simpleFloat;
      this.simpleDouble = simpleDouble;
      this.largeInteger = largeInteger;
   }

   @ProtoField(number = 1, defaultValue = "0")
   int keyColumn;

   @ProtoField(number = 2, defaultValue = "0")
   long simpleLong;

   @ProtoField(number = 3, defaultValue = "0")
   float simpleFloat;

   @ProtoField(number = 4, defaultValue = "0")
   double simpleDouble;

   @ProtoField(number = 5, defaultValue = "0")
   long largeInteger;

   public int simpleInt() {
      return keyColumn;
   }

   public long simpleLong() {
      return simpleLong;
   }

   public float simpleFloat() {
      return simpleFloat;
   }

   public double simpleDouble() {
      return simpleDouble;
   }

   public long largeInteger() {
      return largeInteger;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Numerics numerics = (Numerics) o;
      return keyColumn == numerics.keyColumn
            && simpleLong == numerics.simpleLong
            && Float.compare(simpleFloat, numerics.simpleFloat) == 0
            && Double.compare(simpleDouble, numerics.simpleDouble) == 0
            && largeInteger == numerics.largeInteger;
   }

   @Override
   public int hashCode() {
      return Objects.hash(keyColumn, simpleLong, simpleFloat, simpleDouble, largeInteger);
   }

   @Override
   public String toString() {
      return "Numerics{" +
            "keyColumn=" + keyColumn +
            ", simpleLong=" + simpleLong +
            ", simpleFloat=" + simpleFloat +
            ", simpleDouble=" + simpleDouble +
            ", largeInteger=" + largeInteger +
            '}';
   }
}
