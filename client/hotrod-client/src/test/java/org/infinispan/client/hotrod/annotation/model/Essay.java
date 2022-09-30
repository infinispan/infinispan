package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Essay {

   private String title;

   private String content;

   @ProtoFactory
   public Essay(String title, String content) {
      this.title = title;
      this.content = content;
   }

   @ProtoField(value = 1)
   public String getTitle() {
      return title;
   }

   @ProtoField(value = 2)
   public String getContent() {
      return content;
   }

   @AutoProtoSchemaBuilder(includeClasses = {Essay.class}, schemaPackageName = "essay")
   public interface EssaySchema extends GeneratedSchema {
      EssaySchema INSTANCE = new EssaySchemaImpl();
   }
}
