import org.infinispan.api.annotations.indexing.model.LatLng; // <1>

@Indexed
public record Hiking(@Keyword String name, @GeoField LatLng start, @GeoField LatLng end) { // <2>
}