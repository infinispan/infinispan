package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Color {

   private String name;

   private String description;

   @ProtoField(value = 1)
   @Basic(projectable = true)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(value = 2)
   @Text(analyzer = "keyword")
   public String getDesc1() {
      return description;
   }

   public void setDesc1(String description) {
      this.description = description;
   }

   @ProtoField(value = 3)
   @Text(analyzer = "keyword")
   public String getDesc2() {
      return description;
   }

   public void setDesc2(String description) {
      this.description = description;
   }

   @ProtoField(value = 4)
   @Text
   public String getDesc3() {
      return description;
   }

   public void setDesc3(String description) {
      this.description = description;
   }

   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }

   @ProtoSchema(
         includeClasses = Color.class,
         service = false
   )
   public interface ColorSchema extends GeneratedSchema {
      ColorSchema INSTANCE = new ColorSchemaImpl();
   }
}
