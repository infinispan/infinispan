package org.infinispan.query.dsl.embedded.impl;

import java.util.Locale;

import org.hibernate.search.engine.spatial.DistanceUnit;

public final class DistanceUnitHelper {

   private DistanceUnitHelper() {
   }

   public static DistanceUnit distanceUnit(String value) {
      String lowerCaseValue = value.trim().toLowerCase(Locale.ROOT);
      return switch (lowerCaseValue) {
         case "m", "meters" -> DistanceUnit.METERS;
         case "km", "kilometers" -> DistanceUnit.KILOMETERS;
         case "mi", "miles" -> DistanceUnit.MILES;
         case "yd", "yards" -> DistanceUnit.YARDS;
         case "nm", "nmi", "nauticalmiles" -> DistanceUnit.NAUTICAL_MILES;
         default -> DistanceUnit.METERS;
      };
   }
}
