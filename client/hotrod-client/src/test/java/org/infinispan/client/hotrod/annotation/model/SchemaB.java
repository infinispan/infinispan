package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(includeClasses = ModelB.class, schemaFileName = "model-schema.proto", schemaPackageName = "model")
public interface SchemaB extends GeneratedSchema {

   SchemaB INSTANCE = new SchemaBImpl();

}
