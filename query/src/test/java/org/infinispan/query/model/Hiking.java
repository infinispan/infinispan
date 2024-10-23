package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.GeoField;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.model.LatLng;
import org.infinispan.protostream.annotations.Proto;

@Proto
@Indexed
public record Hiking(@Keyword String name, @GeoField LatLng start, @GeoField LatLng end) {
}
