package org.infinispan.rest;

import java.io.Serializable;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.protostream.annotations.ProtoField;

public class TestClass implements Serializable, JsonSerialization {

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

   @Override
   public Json toJson() {
      return Json.object()
            .set("_type", TestClass.class.getName())
            .set("name", name);
   }
}
