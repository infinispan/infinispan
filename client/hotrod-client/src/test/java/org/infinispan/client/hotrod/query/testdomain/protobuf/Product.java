package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.math.BigInteger;
import java.time.Instant;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.types.java.math.BigIntegerAdapter;

@Indexed
public class Product {

   private final String name;
   private final Long code;
   private final Double price;
   private final String description;
   private final BigInteger purchases;
   private final Instant moment;

   @ProtoFactory
   public Product(String name, Long code, Double price, String description, BigInteger purchases, Instant moment) {
      this.name = name;
      this.code = code;
      this.price = price;
      this.description = description;
      this.purchases = purchases;
      this.moment = moment;
   }

   @ProtoField(value = 1)
   @Keyword(normalizer = "lowercase")
   public String getName() {
      return name;
   }

   @ProtoField(value = 2)
   @Basic
   public Long getCode() {
      return code;
   }

   @ProtoField(value = 3)
   @Basic
   public Double getPrice() {
      return price;
   }

   @ProtoField(value = 4)
   @Text
   public String getDescription() {
      return description;
   }

   @ProtoField(value = 5)
   public BigInteger getPurchases() {
      return purchases;
   }

   @ProtoField(value = 6)
   public Instant getMoment() {
      return moment;
   }

   @ProtoSchema(
         includeClasses = {
               Product.class,
               BigIntegerAdapter.class
         },
         schemaFilePath = "/protostream",
         schemaFileName = "product-store.proto",
         schemaPackageName = "store.product",
         service = false
   )
   public interface ProductSchema extends GeneratedSchema {
      ProductSchema INSTANCE = new ProductSchemaImpl();
   }
}
