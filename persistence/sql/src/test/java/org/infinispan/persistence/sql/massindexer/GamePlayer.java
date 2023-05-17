package org.infinispan.persistence.sql.massindexer;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class GamePlayer {

   private String nick;
   private Integer ranking;
   private String game;
   private String history;

   @ProtoFactory
   public GamePlayer(String nick, Integer ranking, String game, String history) {
      this.nick = nick;
      this.ranking = ranking;
      this.game = game;
      this.history = history;
   }

   @Keyword(normalizer = "lowercase")
   @ProtoField(value = 1)
   public String getNick() {
      return nick;
   }

   @Basic
   @ProtoField(value = 2)
   public Integer getRanking() {
      return ranking;
   }

   @Keyword(normalizer = "lowercase")
   @ProtoField(value = 3)
   public String getGame() {
      return game;
   }

   @Text
   @ProtoField(value = 4)
   public String getHistory() {
      return history;
   }

   @AutoProtoSchemaBuilder(schemaPackageName = "play", includeClasses = GamePlayer.class)
   public interface GamePlayerSchema extends GeneratedSchema {
      GamePlayerSchema INSTANCE = new GamePlayerSchemaImpl();
   }
}
