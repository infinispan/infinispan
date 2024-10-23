package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.GeoPoint;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed
@GeoPoint(fieldName = "departure", projectable = true, sortable = true, marker = "departure")
@GeoPoint(fieldName = "arrival", projectable = true, sortable = true, marker = "arrival")
public record TrainRoute(
      @Keyword(normalizer = "lowercase") String name,
      @Latitude(marker = "departure") Double departureLat,
      @Longitude(marker = "departure") Double departureLon,
      @Latitude(marker = "arrival") Double arrivalLat,
      @Longitude(marker = "arrival") Double arrivalLon
) {
}
