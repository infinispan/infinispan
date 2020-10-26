package org.infinispan.test.integration.data;

import org.infinispan.protostream.annotations.ProtoField;

public class Person {

   @ProtoField(1)
   public String name;

   @ProtoField(2)
   public Integer id;

   public Person() {
   }

   public Person(String name) {
      this.name = name;
   }

   public Person(String name, Integer id) {
      this.name = name;
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public Integer getId() {
      return id;
   }
}
