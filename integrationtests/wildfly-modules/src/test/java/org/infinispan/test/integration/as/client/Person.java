package org.infinispan.test.integration.as.client;

import org.infinispan.protostream.annotations.ProtoField;

public class Person {

   @ProtoField(number = 1)
   public String name;

   public Person(String name) {
      this.name = name;
   }

   public Person() {
   }
}
