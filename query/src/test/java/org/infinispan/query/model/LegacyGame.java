package org.infinispan.query.model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Analyzer;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "legacy-game")
public class LegacyGame {

   private String name;

   private String description;

   private Integer releaseYear;

   @ProtoFactory
   public LegacyGame(String name, String description, Integer releaseYear) {
      this.name = name;
      this.description = description;
      this.releaseYear = releaseYear;
   }

   @Field(store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = "keyword"))
   @ProtoField(1)
   public String getName() {
      return name;
   }

   @Field(analyze = Analyze.YES)
   @Analyzer(definition = "whitespace")
   @ProtoField(2)
   public String getDescription() {
      return description;
   }

   @Field
   @SortableField
   @ProtoField(3)
   public Integer getReleaseYear() {
      return releaseYear;
   }

   @AutoProtoSchemaBuilder(includeClasses = {LegacyGame.class})
   public interface LegacyGameSchema extends GeneratedSchema {
      LegacyGameSchema INSTANCE = new LegacyGameSchemaImpl();
   }
}
