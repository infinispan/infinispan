package org.infinispan.query.queries.faceting;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
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
   @Basic(aggregable = true)
   private int cubicCapacity;

   @ProtoFactory
   public Car(String make, String color, int cubicCapacity) {
      this.color = color;
      this.cubicCapacity = cubicCapacity;
      this.make = make;
   }

   @Text
   @ProtoField(number = 1)
   public String getMake() {
      return make;
   }

   @Basic(projectable = true)
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
