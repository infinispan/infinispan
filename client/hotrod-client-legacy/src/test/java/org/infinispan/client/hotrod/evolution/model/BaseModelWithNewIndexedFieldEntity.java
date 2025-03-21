package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
@ProtoName("Model") // E
public class BaseModelWithNewIndexedFieldEntity implements Model {

    @ProtoField(number = 1)
    @Basic(projectable = true)
    public Integer entityVersion;

    @ProtoField(number = 2)
    public String id;

    @ProtoField(number = 3)
    @Deprecated(forRemoval=true)
    public String name;

    @ProtoField(number = 4)
    @Keyword(normalizer = "lowercase")
    public String newField;

    @Override
    public String getId() {
        return id;
    }

    @ProtoSchema(includeClasses = BaseModelWithNewIndexedFieldEntity.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution", service = false)
    public interface BaseModelWithNewIndexedFieldEntitySchema extends GeneratedSchema {
        BaseModelWithNewIndexedFieldEntitySchema INSTANCE = new BaseModelWithNewIndexedFieldEntitySchemaImpl();
    }
}
