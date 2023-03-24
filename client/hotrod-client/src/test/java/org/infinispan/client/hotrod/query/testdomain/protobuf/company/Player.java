package org.infinispan.client.hotrod.query.testdomain.protobuf.company;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Player {

   private String name;
   private FootballTeam footballTeam;

   @ProtoFactory
   public Player(String name, FootballTeam footballTeam) {
      this.name = name;
      this.footballTeam = footballTeam;
   }

   @ProtoField(value = 1)
   public String getName() {
      return name;
   }

   @ProtoField(value = 2)
   public FootballTeam getFootballTeam() {
      return footballTeam;
   }
}
