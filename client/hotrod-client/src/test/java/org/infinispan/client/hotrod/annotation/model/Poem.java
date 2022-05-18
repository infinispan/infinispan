package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.api.annotations.indexing.option.TermVector;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Poem {

   private Author author;
   private String description;
   private Integer year;

   @ProtoFactory
   public Poem(Author author, String description, Integer year) {
      this.author = author;
      this.description = description;
      this.year = year;
   }

   @Embedded(includeDepth = 2, structure = Structure.NESTED)
   @ProtoField(value = 1)
   public Author getAuthor() {
      return author;
   }

   @Text(projectable = true, analyzer = "whitespace", termVector = TermVector.WITH_OFFSETS)
   @ProtoField(value = 2)
   public String getDescription() {
      return description;
   }

   @Basic(projectable = true, sortable = true, indexNullAs = "1803")
   @ProtoField(value = 3, defaultValue = "1803")
   public Integer getYear() {
      return year;
   }

   @AutoProtoSchemaBuilder(includeClasses = {Poem.class, Author.class}, schemaPackageName = "poem")
   public interface PoemSchema extends GeneratedSchema {
      PoemSchema INSTANCE = new PoemSchemaImpl();
   }
}
