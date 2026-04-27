package test.org.infinispan.spring.starter.remote.schema;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(schemaPackageName = "test.greeting", includeClasses = Greeting.class)
public interface GreetingSchema extends GeneratedSchema {
}
