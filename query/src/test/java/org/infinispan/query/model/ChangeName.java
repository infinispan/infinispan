package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;

@Proto
@Indexed
public record ChangeName(@Basic(name = INDEX_FIELD_NAME) String dataProperty) {

   public static final String DATA_FIELD_NAME = "dataProperty";
   public static final String INDEX_FIELD_NAME = "indexProperty";

   @ProtoSchema(
         includeClasses = ChangeName.class,
         schemaFileName = "change-name.proto",
         schemaPackageName = "bla"
   )
   public interface ChangeNameSchema extends GeneratedSchema {
      ChangeNameSchema INSTANCE = new ChangeNameSchemaImpl();
   }
}
