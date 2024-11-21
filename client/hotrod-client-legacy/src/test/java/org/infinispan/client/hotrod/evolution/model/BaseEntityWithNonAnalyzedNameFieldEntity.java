package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoReserved;

@Indexed
@ProtoName("Model") // H
@ProtoReserved(numbers = 3, names = "name")
public class BaseEntityWithNonAnalyzedNameFieldEntity implements Model {

   @ProtoField(number = 1)
   @Basic(projectable = true)
   public Integer entityVersion;

   @ProtoField(number = 2)
   public String id;

   @ProtoField(number = 4)
   @Basic(projectable = true)
   public String analyzed;

   @Override
   public String getId() {
      return id;
   }

   @AutoProtoSchemaBuilder(includeClasses = BaseEntityWithNonAnalyzedNameFieldEntity.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution", service = false)
   public interface BaseEntityWithNonAnalyzedNameFieldEntitySchema extends GeneratedSchema {
      BaseEntityWithNonAnalyzedNameFieldEntitySchema INSTANCE = new BaseEntityWithNonAnalyzedNameFieldEntitySchemaImpl();
   }
}
