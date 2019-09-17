package org.infinispan.query.queries.faceting;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Facet;
import org.hibernate.search.annotations.FacetEncodingType;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.IntegerBridge;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Hardy Ferentschik
 */
@Indexed(index = "car")
public class Car {

   @Field(analyze = Analyze.NO)
   private String color;

   @Field(store = Store.YES)
   private String make;

   @Field(analyze = Analyze.NO, bridge = @FieldBridge(impl = IntegerBridge.class))
   @Facet(encoding = FacetEncodingType.STRING)
   private int cubicCapacity;

   @ProtoFactory
   public Car(String make, String color, int cubicCapacity) {
      this.color = color;
      this.cubicCapacity = cubicCapacity;
      this.make = make;
   }

   @ProtoField(number = 1)
   public String getMake() {
      return make;
   }

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
