package org.infinispan.client.hotrod.query.key.tutorial;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(includeClasses = {PersonKey.class, Author.class, Person.class}, schemaFileName = "TutorialSchema.proto",
      schemaPackageName = "tutorial")
public interface TutorialSchema extends GeneratedSchema {

   TutorialSchema TUTORIAL_SCHEMA = new TutorialSchemaImpl();

}
