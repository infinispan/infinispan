package org.infinispan.client.hotrod.evolution.model;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.client.hotrod.annotation.model.Model;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed // Uses the old annotations so we can test that migration from the old annotations to new ones
@ProtoName("Model") // J
public class BaseModelWithNameFieldAnalyzedEntityOldAnnotations implements Model {

    @ProtoField(number = 1)
    // Uses the old annotations so we can test that migration from the old annotations to new ones
    @ProtoDoc("@Field(index = Index.YES, store = Store.YES)")
    public Integer entityVersion;

    @ProtoField(number = 2)
    public String id;

    @ProtoField(number = 3)
    // Uses the old annotations so we can test that migration from the old annotations to new ones
    @ProtoDoc("@Field(index = Index.YES, store = Store.YES, analyze = Analyze.YES, analyzer = @Analyzer(definition = \"standard\"))")
    public String nameAnalyzed;

    @Override
    public String getId() {
        return id;
    }

    @AutoProtoSchemaBuilder(includeClasses = BaseModelWithNameFieldAnalyzedEntityOldAnnotations.class, schemaFileName = "evolution-schema.proto", schemaPackageName = "evolution")
    public interface BaseModelWithNameFieldAnalyzedEntityOldAnnotationsSchema extends GeneratedSchema {
       BaseModelWithNameFieldAnalyzedEntityOldAnnotationsSchema INSTANCE = new BaseModelWithNameFieldAnalyzedEntityOldAnnotationsSchemaImpl();
    }
}
