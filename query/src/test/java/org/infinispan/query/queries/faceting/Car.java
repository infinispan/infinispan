package org.infinispan.query.queries.faceting;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.engine.backend.types.Aggregable;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.GenericField;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Hardy Ferentschik
 */
@Indexed(index = "car")
public class Car {

   private String color;

   private String make;

   // the Search6's aggregation is the new HS5's faceting
   @GenericField(aggregable = Aggregable.YES)
   private int cubicCapacity;

   @ProtoFactory
   public Car(String make, String color, int cubicCapacity) {
      this.color = color;
      this.cubicCapacity = cubicCapacity;
      this.make = make;
   }

   @Field(store = Store.YES)
   @ProtoField(number = 1)
   public String getMake() {
      return make;
   }

   @Field(analyze = Analyze.NO)
   @ProtoField(number = 2)
   public String getColor() {
      return color;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public int getCubicCapacity() {
      return cubicCapacity;
   }

   @Override
   public String toString() {
      return "Car{color='" + color + "', make='" + make + "', cubicCapacity=" + cubicCapacity + '}';
   }
}
