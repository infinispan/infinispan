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
@ProtoName("Model") // K
public class BaseModelIndexAttributesEntity implements Model {

    @ProtoField(number = 1)
    @Basic(projectable = true)
    public Integer entityVersion;

    @ProtoField(number = 2)
    @Basic(sortable = true)
    public String id;

    @ProtoField(number = 3)
    @Basic(aggregable = true)
    public Integer number;

    @ProtoField(number = 4)
    @Basic()
    public String name;

    @ProtoField(number = 5)
    @Keyword(normalizer = "lowercase")
    public String normalizedField;

    @ProtoField(number = 6)
    @Text()
    public String analyzedField;

    @Override
    public String getId() {
        return id;
    }

    @AutoProtoSchemaBuilder(includeClasses = BaseModelIndexAttributesEntity.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution")
    public interface BaseModelIndexAttributesEntitySchema extends GeneratedSchema {
        BaseModelIndexAttributesEntitySchema INSTANCE = new BaseModelIndexAttributesEntitySchemaImpl();
    }
}
