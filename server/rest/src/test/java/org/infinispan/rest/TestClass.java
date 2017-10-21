package org.infinispan.rest;

import java.io.Serializable;

public class TestClass implements Serializable {

   private String name;

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
