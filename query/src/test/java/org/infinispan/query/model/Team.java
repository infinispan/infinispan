package org.infinispan.query.model;

import java.util.List;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.option.Structure;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;

@Proto
@Indexed
public record Team(@Basic String name, @Embedded(structure = Structure.NESTED) List<Player> firstTeam,
                   @Embedded(structure = Structure.FLATTENED)List<Player> replacements) {

   @ProtoSchema(includeClasses = {Team.class, Player.class}, schemaPackageName = "model")
   public interface TeamSchema extends GeneratedSchema {
      TeamSchema INSTANCE = new TeamSchemaImpl();
   }

}
