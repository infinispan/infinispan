package org.infinispan.query.model;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class NonIndexedGame {

   private String name;

   private String description;

   @ProtoFactory
   public NonIndexedGame(String name, String description) {
      this.name = name;
      this.description = description;
   }

   @ProtoField(1)
   public String getName() {
      return name;
   }

   @ProtoField(2)
   public String getDescription() {
      return description;
   }

   @Override
   public String toString() {
      return "Game{" +
            "name='" + name + '\'' +
            ", description='" + description + '\'' +
            '}';
   }
}
