package org.infinispan.client.hotrod.query.testdomain.protobuf.company;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = {
            FootballTeam.class,
            Player.class
      },
      schemaPackageName = "org.football",
      schemaFileName = "football.proto",
      schemaFilePath = "org/infinispan/client/hotrod",
      service = false
)
public interface FootballSchema extends GeneratedSchema {
}
