package org.infinispan.commons.marshall.proto;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * {@link org.infinispan.protostream.SerializationContextInitializer} that includes all classes that are supported for
 * use by a user in their own {@link org.infinispan.protostream.SerializationContext}. When creating a {@link
 * org.infinispan.protostream.SerializationContextInitializer} if any of the included classes marshaller any of the
 * classes in this initializer, then it's necessary to add this {@link org.infinispan.protostream.SerializationContextInitializer}
 * as a dependency via the {@link AutoProtoSchemaBuilder#dependsOn()} field.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
      includeClasses = RuntimeMarshallableWrapper.class,
      schemaFileName = "commons.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.commons")
interface UserSerializationContextInternalizer extends SerializationContextInitializer {
}
