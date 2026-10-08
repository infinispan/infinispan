package org.infinispan.protostream.sampledomain;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed(index = "play")
public class Game {

   private final String name;

   private final String description;

   @ProtoFactory
   public Game(String name, String description) {
      this.name = name;
      this.description = description;
   }

   @Keyword(projectable = true, sortable = true)
   @ProtoField(1)
   public String getName() {
      return name;
   }

   @Text
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

   @ProtoSchema(includeClasses = {Game.class, NonIndexedGame.class, GameKey.class})
   public interface GameSchema extends GeneratedSchema {
      GameSchema INSTANCE = new GameSchemaImpl();
   }

   public static Map<String, Game> data() {
      Map<String, Game> map = new LinkedHashMap<>();
      map.put("g1", new Game("Civilization", "The best strategy game"));
      map.put("g2", new Game("Doom", "First person shooter classic"));
      map.put("g3", new Game("Tetris", "Puzzle game with blocks"));
      return map;
   }
}
