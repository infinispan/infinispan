package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Model") // B
public class BaseModelWithNameFieldIndexedEntity implements Model {

    @ProtoField(number = 1)
    @Basic(projectable = true)
    public Integer entityVersion;

    @ProtoField(number = 2)
    public String id;

    @ProtoField(number = 3)
    @Basic(projectable = true, sortable = true)
    public String name;

    @Override
    public String getId() {
        return id;
    }

    @AutoProtoSchemaBuilder(includeClasses = BaseModelWithNameFieldIndexedEntity.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution")
    public interface BaseModelWithNameFieldIndexedEntitySchema extends GeneratedSchema {
        BaseModelWithNameFieldIndexedEntitySchema INSTANCE = new BaseModelWithNameFieldIndexedEntitySchemaImpl();
    }
}
