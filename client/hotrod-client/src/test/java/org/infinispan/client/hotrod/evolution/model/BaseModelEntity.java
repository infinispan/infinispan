package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Model") // A
public class BaseModelEntity implements Model {

    @ProtoField(number = 1)
    @Basic(projectable = true)
    public Integer entityVersion;

    @ProtoField(number = 2)
    public String id;

    @ProtoField(number = 3)
    public String name;

    @Override
    public String getId() {
        return id;
    }

    @AutoProtoSchemaBuilder(includeClasses = BaseModelEntity.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution")
    public interface BaseModelEntitySchema extends GeneratedSchema {
        BaseModelEntitySchema INSTANCE = new BaseModelEntitySchemaImpl();
    }
}
