package org.infinispan.test.integration.as.client;

import org.infinispan.protostream.annotations.ProtoField;

public class Person {

   @ProtoField(number = 1)
   public String name;

   @ProtoField(number = 2)
   public Integer id;

   public Person(String name) {
      this.name = name;
   }

   public Person(String name, Integer id) {
      this.name = name;
      this.id = id;
   }

   public Person() {
   }
}
