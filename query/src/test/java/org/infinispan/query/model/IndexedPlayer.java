package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@Proto
@Indexed
public record IndexedPlayer(@Basic(projectable = true, aggregable = true, sortable = true) String name, @Basic(projectable = true, aggregable = true, sortable = true) String color, @Basic(projectable = true, aggregable = true, sortable = true) Integer number) {

    @ProtoSchema(
            includeClasses = { IndexedPlayer.class },
            schemaFileName = "player_indexed.proto",
            schemaPackageName = "pagg",
            syntax = ProtoSyntax.PROTO3
    )
    public interface PlayerSchema extends GeneratedSchema {
        PlayerSchema INSTANCE = new PlayerSchemaImpl();
    }
}
