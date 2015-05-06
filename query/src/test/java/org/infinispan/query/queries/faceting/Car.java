package org.infinispan.query.queries.faceting;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Facet;
import org.hibernate.search.annotations.FacetEncodingType;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.IntegerBridge;

/**
 * @author Hardy Ferentschik
 */
@Indexed(index = "car")
public class Car implements Serializable {

   @Field(analyze = Analyze.NO)
   private String color;

   @Field(store = Store.YES)
   private String make;
   
   @Field(analyze = Analyze.NO, bridge = @FieldBridge(impl = IntegerBridge.class))
   @Facet(encoding = FacetEncodingType.STRING)
   private int cubicCapacity;

   public Car(String make, String color, int cubicCapacity) {
      this.color = color;
      this.cubicCapacity = cubicCapacity;
      this.make = make;
   }

   public String getMake() {
      return make;
   }

   @Override
   public String toString() {
      return "Car" + "{color='" + color + '\'' + ", make='" + make + '\'' + ", cubicCapacity=" + cubicCapacity + '}';
   }
}
