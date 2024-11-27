package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

public class Essay {

   private final String title;

   private final String content;

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

   @ProtoSchema(includeClasses = {Essay.class}, schemaPackageName = "essay")
   public interface EssaySchema extends GeneratedSchema {
      EssaySchema INSTANCE = new EssaySchemaImpl();
   }
}
