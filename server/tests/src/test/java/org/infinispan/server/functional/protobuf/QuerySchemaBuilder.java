package org.infinispan.server.functional.protobuf;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(schemaPackageName = "tutorial", includeClasses = Person.class)
public interface QuerySchemaBuilder extends GeneratedSchema {
}
