package org.infinispan.protostream.sampledomain;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.GeoPoint;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Latitude;
import org.infinispan.api.annotations.indexing.Longitude;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@Proto
@Indexed
@GeoPoint(fieldName = "location", projectable = true, sortable = true)
public record Restaurant(
      @Keyword(normalizer = "lowercase", projectable = true, sortable = true) String name,
      @Text String description,
      @Text String address,
      @Latitude(fieldName = "location") Double latitude,
      @Longitude(fieldName = "location") Double longitude,
      @Basic(projectable = true) Float score
) {

   public static Map<String, Restaurant> data() {
      Map<String, Restaurant> data = new LinkedHashMap<>();
      data.put("La Locanda di Pietro", new Restaurant("La Locanda di Pietro",
            "Roman-style pasta dishes & Lazio region wines at a cozy traditional trattoria with a shaded terrace.",
            "Via Sebastiano Veniero, 28/c, 00192 Roma RM", 41.907903484609356, 12.45540543756422, 4.6f));
      data.put("Scialla The Original Street Food", new Restaurant("Scialla The Original Street Food",
            "Pastas & traditional pizza pies served in an unassuming eatery with vegetarian options.",
            "Vicolo del Farinone, 27, 00193 Roma RM", 41.90369455835456, 12.459566517195528, 4.7f));
      data.put("Trattoria Pizzeria Gli Archi", new Restaurant("Trattoria Pizzeria Gli Archi",
            "Traditional trattoria with exposed brick walls, serving up antipasti, pizzas & pasta dishes.",
            "Via Sebastiano Veniero, 26, 00192 Roma RM", 41.907930453801285, 12.455204785977637, 4.0f));
      data.put("Alla Bracioleria Gracchi Restaurant", new Restaurant("Alla Bracioleria Gracchi Restaurant",
            "", "Via dei Gracchi, 19, 00192 Roma RM", 41.907129402661795, 12.458927251586584, 4.7f));
      data.put("Magazzino Scipioni", new Restaurant("Magazzino Scipioni",
            "Contemporary venue with a focus on unique wines & seasonal Italian plates, plus a bottle shop.",
            "Via degli Scipioni, 30, 00192 Roma RM", 41.90817843995448, 12.457118458698043, 4.6f));
      data.put("Dal Toscano Restaurant", new Restaurant("Dal Toscano Restaurant",
            "Rich pastas, signature steaks & classic Tuscan dishes, plus Chianti wines, at a venerable trattoria.",
            "Via Germanico, 58-60, 00192 Roma RM", 41.90785274056548, 12.45822050287784, 4.2f));
      data.put("Il Ciociaro", new Restaurant("Il Ciociaro",
            "Long-running, old-school restaurant plating traditional staples, from carbonara to tiramisu.",
            "Via Barletta, 21, 00192 Roma RM", 41.91038657525997, 12.458851939120656, 4.2f));
      return data;
   }

   @ProtoSchema(
         includeClasses = {Restaurant.class, TrainRoute.class},
         schemaFileName = "geo.proto",
         schemaPackageName = "geo",
         syntax = ProtoSyntax.PROTO3
   )
   public interface RestaurantSchema extends GeneratedSchema {
      RestaurantSchema INSTANCE = new RestaurantSchemaImpl();
   }
}
