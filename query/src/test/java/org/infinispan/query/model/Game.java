package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "play")
public class Game {

   private String name;

   private String description;

   @ProtoFactory
   public Game(String name, String description) {
      this.name = name;
      this.description = description;
   }

   @Keyword(projectable = true)
   @ProtoField(1)
   public String getName() {
      return name;
   }

   @Text
   @ProtoField(2)
   public String getDescription() {
      return description;
   }

   @AutoProtoSchemaBuilder(includeClasses = Game.class)
   public interface GameSchema extends GeneratedSchema {
      GameSchema INSTANCE = new GameSchemaImpl();
   }
}
