package org.infinispan.protostream.sampledomain;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;

@Proto
@Indexed
public record NumericAggregation(
      @Basic(projectable = true, aggregable = true, sortable = true) String category,
      @Basic(projectable = true, aggregable = true) Integer integerValue,
      @Basic(projectable = true, aggregable = true) Long longValue) {

   @ProtoSchema(includeClasses = NumericAggregation.class, schemaPackageName = "aggregation", service = false)
   public interface NumericAggregationSchema extends GeneratedSchema {
      NumericAggregationSchema INSTANCE = new NumericAggregationSchemaImpl();
   }
}
