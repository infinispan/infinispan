@Proto
@Indexed
@GeoPoint(fieldName = "location", projectable = true, sortable = true) // <1>
public record Restaurant(
      @Keyword(normalizer = "lowercase", projectable = true, sortable = true) String name,
      @Text String description,
      @Text String address,
      @Latitude(fieldName = "location") Double latitude, // <2>
      @Longitude(fieldName = "location") Double longitude, // <3>
      @Basic Float score
) {

   @ProtoSchema( // <4>
         includeClasses = { Restaurant.class, TrainRoute.class }, // <5>
         schemaFileName = "geo.proto",
         schemaPackageName = "geo",
         syntax = ProtoSyntax.PROTO3
   )
   public interface RestaurantSchema extends GeneratedSchema {
      RestaurantSchema INSTANCE = new RestaurantSchemaImpl();
   }
}
