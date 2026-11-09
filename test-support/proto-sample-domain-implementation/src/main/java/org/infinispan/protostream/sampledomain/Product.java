package org.infinispan.protostream.sampledomain;

import java.math.BigInteger;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

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

    public static Map<String, Product> data() {
       Product p1 = new Product("Pilsner Urquell", 2178129121111L, 23.78,
             "Pilsner Urquell is a lager beer brewed by the Pilsner Urquell Brewery in Plzeň, Czech Republic. Pilsner Urquell was the world's first pale lager, and its popularity meant it was much copied, and named pils, pilsner or pilsener. It is hopped with Saaz hops, a noble hop variety which is a key element in its flavour profile, as is the use of soft water.",
             BigInteger.valueOf((long) Integer.MAX_VALUE + 1000), Instant.ofEpochSecond(1675769531, 123000000));
       Product p2 = new Product("Lavazza Coffee", 178128739123L, 10.99,
             "Lavazza imports coffee from around the world, including Brazil, Colombia, Guatemala, Costa Rica, Honduras, Uganda, Indonesia, the United States and Mexico.\n Branded as \"Italy's Favourite Coffee,\" the company claims that 16 million out of the 20 million coffee purchasing families in Italy choose Lavazza.",
             BigInteger.valueOf((long) Integer.MAX_VALUE + 1000), Instant.ofEpochSecond(1675769531, 123000000));
       Product p3 = new Product("Puma Backpack", 21233131131L, 40.99,
             "Lightweight and practical gym bag made of durable material, which can be carried as a backpack. This classic gym sack slings easily over the shoulder and for carrying smaller loads.",
             BigInteger.valueOf((long) Integer.MAX_VALUE + 1000), Instant.ofEpochSecond(1675769531, 123000000));
       Map<String, Product> data = new LinkedHashMap<>();
       data.put("1", p1);
       data.put("2", p2);
       data.put("3", p3);
       return data;
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
