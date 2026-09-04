package org.infinispan.core.test.jupiter.proto;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Product {

   private final String name;
   private final double price;

   @ProtoFactory
   public Product(String name, double price) {
      this.name = name;
      this.price = price;
   }

   @ProtoField(1)
   public String getName() {
      return name;
   }

   @ProtoField(value = 2, defaultValue = "0")
   public double getPrice() {
      return price;
   }
}
