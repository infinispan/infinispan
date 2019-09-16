package org.infinispan.rest;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoField;

public class TestClass implements Serializable {

   private String name;

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Override
   public String toString() {
      return "TestClass{" +
            "name='" + name + '\'' +
            '}';
   }
}
