package org.infinispan.protostream.sampledomain;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.types.java.CommonTypes;

@Indexed(index = "play")
public class Game {

   public static final String ENTITY_NAME = "org.infinispan.protostream.sampledomain.Game";

   private final String name;
   private final String description;
   private final int rating;
   private final double price;
   private final List<String> categories;
   private final LocalDateTime releaseDate;

   public Game(String name, String description) {
      this(name, description, 0, 0.0, new ArrayList<>(), null);
   }

   @ProtoFactory
   public Game(String name, String description, int rating, double price, List<String> categories, LocalDateTime releaseDate) {
      this.name = name;
      this.description = description;
      this.rating = rating;
      this.price = price;
      this.categories = categories;
      this.releaseDate = releaseDate;
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

   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 3, defaultValue = "0")
   public int getRating() {
      return rating;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 4, defaultValue = "0.0")
   public double getPrice() {
      return price;
   }

   @ProtoField(5)
   public List<String> getCategories() {
      return categories;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(6)
   public LocalDateTime getReleaseDate() {
      return releaseDate;
   }

   @Override
   public String toString() {
      return "Game{" +
            "name='" + name + '\'' +
            ", description='" + description + '\'' +
            ", rating=" + rating +
            ", price=" + price +
            ", categories=" + categories +
            ", releaseDate=" + releaseDate +
            '}';
   }

   @ProtoSchema(includeClasses = {Game.class, NonIndexedGame.class, GameKey.class}, dependsOn = CommonTypes.class)
   public interface GameSchema extends GeneratedSchema {
      GameSchema INSTANCE = new GameSchemaImpl();
   }

   public static Map<String, Game> data() {
      Map<String, Game> map = new LinkedHashMap<>();
      map.put("g1", new Game("Civilization", "The best strategy game", 90, 59.99,
            List.of("strategy", "turn-based", "4x"), LocalDate.of(1991, 9, 15).atStartOfDay()));
      map.put("g2", new Game("Doom", "First person shooter classic", 85, 39.99,
            List.of("shooter", "action", "fps"), LocalDate.of(1993, 3, 10).atStartOfDay()));
      map.put("g3", new Game("Tetris", "Puzzle game with blocks", 95, 19.99,
            List.of("puzzle", "arcade", "classic"), LocalDate.of(1984, 6, 6).atStartOfDay()));
      return map;
   }
}
