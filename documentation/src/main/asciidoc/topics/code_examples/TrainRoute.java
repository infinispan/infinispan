@Proto // <1>
@Indexed
@GeoPoint(fieldName = "departure", projectable = true, sortable = true) // <2>
@GeoPoint(fieldName = "arrival", projectable = true, sortable = true) // <3>
public record TrainRoute(
      @Keyword(normalizer = "lowercase") String name,
      @Latitude(fieldName = "departure") Double departureLat, // <4>
      @Longitude(fieldName = "departure") Double departureLon, // <5>
      @Latitude(fieldName = "arrival") Double arrivalLat, // <6>
      @Longitude(fieldName = "arrival") Double arrivalLon // <7>
) {
}