package org.infinispan.query.dsl.embedded.testdomain;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.GeoPoint;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
@GeoPoint(fieldName = "start", projectable = true, sortable = true)
@GeoPoint(fieldName = "end", projectable = true, sortable = true)
public class FlightRoute {

   @Basic(projectable = true, sortable = true)
   @ProtoField(1)
   public final String name;

   @Latitude(fieldName = "start")
   @ProtoField(2)
   public final Double startLat;

   @Longitude(fieldName = "start")
   @ProtoField(3)
   public final Double startLon;

   @Latitude(fieldName = "end")
   @ProtoField(4)
   public final Double endLat;

   @Longitude(fieldName = "end")
   @ProtoField(5)
   public final Double endLon;

   @ProtoFactory
   public FlightRoute(String name, Double startLat, Double startLon, Double endLat, Double endLon) {
      this.name = name;
      this.startLat = startLat;
      this.startLon = startLon;
      this.endLat = endLat;
      this.endLon = endLon;
   }

   @Override
   public String toString() {
      return "FlightRoute{" +
            "name='" + name + '\'' +
            ", startLat=" + startLat +
            ", startLon=" + startLon +
            ", endLat=" + endLat +
            ", endLon=" + endLon +
            '}';
   }
}
