package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Structure {

   private final String code;
   private final String description;
   private final Integer value;
   private final StructureKey nested;

   @ProtoFactory
   public Structure(String code, String description, Integer value, StructureKey nested) {
      this.code = code;
      this.description = description;
      this.value = value;
      this.nested = nested;
   }

   @ProtoField(1)
   @Basic
   public String getCode() {
      return code;
   }

   @ProtoField(2)
   @Text
   public String getDescription() {
      return description;
   }

   @ProtoField(3)
   @Basic
   public Integer getValue() {
      return value;
   }

   @Embedded
   @ProtoField(4)
   public StructureKey getNested() {
      return nested;
   }

   @ProtoSchema(includeClasses = { Structure.class, StructureKey.class }, schemaPackageName = "model")
   public interface StructureSchema extends GeneratedSchema {
      StructureSchema INSTANCE = new StructureSchemaImpl();
   }
}
