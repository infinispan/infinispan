package org.infinispan.query.model;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = Developer.class,
      schemaFileName = "model.proto",
      schemaPackageName = "org.infinispan.query.model"
)
public interface ModelSchema extends GeneratedSchema {
}
