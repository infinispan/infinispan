package org.infinispan.client.hotrod.query.testdomain.protobuf.company;

import java.util.List;

import org.infinispan.protostream.annotations.ProtoField;

public class FootballTeam {

   private String name;
   private List<Player> players;

   @ProtoField(value = 1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(value = 2)
   public List<Player> getPlayers() {
      return players;
   }

   public void setPlayers(List<Player> players) {
      this.players = players;
   }
}
