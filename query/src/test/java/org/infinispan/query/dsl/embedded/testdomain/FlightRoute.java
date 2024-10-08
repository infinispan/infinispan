package org.infinispan.query.dsl.embedded.testdomain;

import org.hibernate.search.engine.backend.types.Projectable;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.mapper.pojo.bridge.builtin.annotation.GeoPointBinding;
import org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Latitude;
import org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Longitude;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
@GeoPointBinding(fieldName = "start", markerSet = "start", projectable = Projectable.YES, sortable = Sortable.YES)
@GeoPointBinding(fieldName = "end", markerSet = "end", projectable = Projectable.YES, sortable = Sortable.YES)
public class FlightRoute {

   @Basic(projectable = true, sortable = true)
   @ProtoField(1)
   public String name;

   @Latitude(markerSet = "start")
   @ProtoField(2)
   public Double startLat;

   @Longitude(markerSet = "start")
   @ProtoField(3)
   public Double startLon;

   @Latitude(markerSet = "end")
   @ProtoField(4)
   public Double endLat;

   @Longitude(markerSet = "end")
   @ProtoField(5)
   public Double endLon;

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
