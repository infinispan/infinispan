package org.infinispan.protostream.sampledomain;

import java.util.LinkedHashMap;
import java.util.Map;

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
      @Keyword(normalizer = "lowercase", projectable = true) String name,
      @Latitude(fieldName = "departure") Double departureLat,
      @Longitude(fieldName = "departure") Double departureLon,
      @Latitude(fieldName = "arrival") Double arrivalLat,
      @Longitude(fieldName = "arrival") Double arrivalLon
) {

   public static Map<String, TrainRoute> data() {
      double romeLat = 41.8967, romeLon = 12.4822;
      double bolognaLat = 44.4949, bolognaLon = 11.3426;
      double milanLat = 45.4685, milanLon = 9.1824;
      double comoLat = 45.8064, comoLon = 9.0852;
      double veniceLat = 45.4404, veniceLon = 12.3160;
      double selvaLat = 46.5560, selvaLon = 11.7559;
      Map<String, TrainRoute> data = new LinkedHashMap<>();
      data.put("Rome-Milan", new TrainRoute("Rome-Milan", romeLat, romeLon, milanLat, milanLon));
      data.put("Bologna-Selva", new TrainRoute("Bologna-Selva", bolognaLat, bolognaLon, selvaLat, selvaLon));
      data.put("Milan-Como", new TrainRoute("Milan-Como", milanLat, milanLon, comoLat, comoLon));
      data.put("Bologna-Venice", new TrainRoute("Bologna-Venice", bolognaLat, bolognaLon, veniceLat, veniceLon));
      return data;
   }
}
