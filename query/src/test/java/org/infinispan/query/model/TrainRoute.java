package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.GeoPoint;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed
@GeoPoint(fieldName = "departure", projectable = true, sortable = true)
@GeoPoint(fieldName = "arrival", projectable = true, sortable = true)
public record TrainRoute(
      @Keyword(normalizer = "lowercase") String name,
      @Latitude(fieldName = "departure") Double departureLat,
      @Longitude(fieldName = "departure") Double departureLon,
      @Latitude(fieldName = "arrival") Double arrivalLat,
      @Longitude(fieldName = "arrival") Double arrivalLon
) {
}
