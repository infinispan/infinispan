import org.infinispan.commons.api.query.geo.LatLng; // <1>

@Proto
@Indexed
public record ProtoHiking(@Keyword String name, @GeoField LatLng start, @GeoField LatLng end) { // <2>

   @ProtoSchema(
         dependsOn = LatLng.LatLngSchema.class, // <3>
         includeClasses = ProtoHiking.class,
         schemaFileName = "hiking.proto",
         schemaPackageName = "geo",
         syntax = ProtoSyntax.PROTO3
   )
   public interface ProtoHikingSchema extends GeneratedSchema {
      ProtoHikingSchema INSTANCE = new ProtoHikingSchemaImpl();
   }
}