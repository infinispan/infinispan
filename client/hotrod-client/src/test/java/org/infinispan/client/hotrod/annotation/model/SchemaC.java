package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(includeClasses = { ModelC.class, Image.class }, schemaFileName = "model-schema.proto", schemaPackageName = "model")
public interface SchemaC extends GeneratedSchema {

   SchemaC INSTANCE = new SchemaCImpl();

}
