package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Sale {

   private final String id;

   private final String code;

   private final String status;

   private final Integer day;

   @ProtoFactory
   public Sale(String id, String code, String status, Integer day) {
      this.id = id;
      this.code = code;
      this.status = status;
      this.day = day;
   }

   @Basic
   @ProtoField(1)
   public String getId() {
      return id;
   }

   @Basic
   @ProtoField(2)
   public String getCode() {
      return code;
   }

   @Basic(sortable = true, aggregable = true)
   @ProtoField(3)
   public String getStatus() {
      return status;
   }

   @Basic
   @ProtoField(4)
   public Integer getDay() {
      return day;
   }

   @Override
   public String toString() {
      return "Sale{" +
            "id='" + id + '\'' +
            ", code='" + code + '\'' +
            ", status='" + status + '\'' +
            ", day=" + day +
            '}';
   }

   @AutoProtoSchemaBuilder(includeClasses = Sale.class)
   public interface SaleSchema extends GeneratedSchema {
      SaleSchema INSTANCE = new SaleSchemaImpl();
   }
}
