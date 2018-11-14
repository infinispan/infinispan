package org.infinispan.rest;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

public class TestClass implements Serializable, ExternalPojo {

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
