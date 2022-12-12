package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Model") // L
public class BaseModelIndexAttributesChangedEntity implements Model {

    @ProtoField(number = 1)
    @Basic
    public Integer entityVersion;

    @ProtoField(number = 2)
    @Basic
    public String id;

    @ProtoField(number = 3)
    @Basic
    public Integer number;

    @ProtoField(number = 4)
    @Basic(projectable = true, sortable = true, aggregable = true)
    public String name;

    @ProtoField(number = 5)
    @Keyword
    public String normalizedField;

    @ProtoField(number = 6)
    @Text(analyzer = "keyword")
    public String analyzedField;

    @Override
    public String getId() {
        return id;
    }

    @AutoProtoSchemaBuilder(includeClasses = BaseModelIndexAttributesChangedEntity.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution")
    public interface BaseModelIndexAttributesChangedEntitySchema extends GeneratedSchema {
        BaseModelIndexAttributesChangedEntitySchema INSTANCE = new BaseModelIndexAttributesChangedEntitySchemaImpl();
    }
}
