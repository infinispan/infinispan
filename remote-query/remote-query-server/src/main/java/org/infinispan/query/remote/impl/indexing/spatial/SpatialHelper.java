package org.infinispan.query.remote.impl.indexing.spatial;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.exception.AssertionFailure;
import org.hibernate.search.spatial.Coordinates;
import org.hibernate.search.spatial.impl.GeometricConstants;
import org.hibernate.search.spatial.impl.Point;
import org.hibernate.search.spatial.impl.Rectangle;

/**
 * Spatial fields, ids generator and geometric calculation methods for use in SpatialFieldBridge
 *
 * @author Nicolas Helleringer
 * @author Mathieu Perez
 * @see org.hibernate.search.spatial.SpatialFieldBridgeByHash
 * @see org.hibernate.search.spatial.SpatialFieldBridgeByRange
 */
public abstract class SpatialHelper {

   private static final double LOG2 = Math.log(2);

   private static final String SPATIAL_FIELD_SUFFIX = "_HSSI_";
   private static final String SPATIAL_LATITUDE_SUFFIX = SPATIAL_FIELD_SUFFIX + "Latitude";
   private static final String SPATIAL_LONGITUDE_SUFFIX = SPATIAL_FIELD_SUFFIX + "Longitude";

   /**
    * Private constructor locking down utility class
    */
   private SpatialHelper() {
   }

   /**
    * Generate a Cell Index on one axis
    *
    * @param coordinate       position to compute the Index for
    * @param range            range of the axis {@literal (-pi,pi)/(-90,90) => 2*pi/180} e.g
    * @param spatialHashLevel Hox many time the range has been split in two
    * @return the cell index on the axis
    */
   public static int getCellIndex(double coordinate, double range, int spatialHashLevel) {
      return (int) Math.floor(Math.pow(2, spatialHashLevel) * coordinate / range);
   }

   /**
    * Generate a spatial hash cell id (with both cell index on both dimension in it) for a position
    *
    * @param point            position to compute the spatial hash cell id for
    * @param spatialHashLevel Hox many time the dimensions have been split in two
    * @return the cell id for the point at the given spatial hash level
    */
   public static String getSpatialHashCellId(Point point, int spatialHashLevel) {
      double[] indexablesCoordinates = projectToIndexSpace(point);
      int longitudeCellIndex = getCellIndex(
            indexablesCoordinates[0],
            GeometricConstants.PROJECTED_LONGITUDE_RANGE,
            spatialHashLevel
      );
      int latitudeCellIndex = getCellIndex(
            indexablesCoordinates[1],
            GeometricConstants.PROJECTED_LATITUDE_RANGE,
            spatialHashLevel
      );
      return formatSpatialHashCellId(longitudeCellIndex, latitudeCellIndex);
   }

   /**
    * Generate a spatial hash cell ids list covered by a bounding box
    *
    * @param lowerLeft        lower left corner of the bounding box
    * @param upperRight       upper right corner of the bounding box
    * @param spatialHashLevel spatial hash level of the wanted cell ids
    * @return List of ids of the cells containing the point
    */
   public static List<String> getSpatialHashCellsIds(Point lowerLeft, Point upperRight, int spatialHashLevel) {
      double[] projectedLowerLeft = projectToIndexSpace(lowerLeft);
      int lowerLeftXIndex = getCellIndex(
            projectedLowerLeft[0],
            GeometricConstants.PROJECTED_LONGITUDE_RANGE,
            spatialHashLevel
      );
      int lowerLeftYIndex = getCellIndex(
            projectedLowerLeft[1],
            GeometricConstants.PROJECTED_LATITUDE_RANGE,
            spatialHashLevel
      );

      double[] projectedUpperRight = projectToIndexSpace(upperRight);
      int upperRightXIndex = getCellIndex(
            projectedUpperRight[0],
            GeometricConstants.PROJECTED_LONGITUDE_RANGE,
            spatialHashLevel
      );
      int upperRightYIndex = getCellIndex(
            projectedUpperRight[1],
            GeometricConstants.PROJECTED_LATITUDE_RANGE,
            spatialHashLevel
      );

      double[] projectedLowerRight = projectToIndexSpace(Point.fromDegrees(lowerLeft.getLatitude(), upperRight.getLongitude()));
      int lowerRightXIndex = getCellIndex(
            projectedLowerRight[0],
            GeometricConstants.PROJECTED_LONGITUDE_RANGE,
            spatialHashLevel
      );
      int lowerRightYIndex = getCellIndex(
            projectedLowerRight[1],
            GeometricConstants.PROJECTED_LATITUDE_RANGE,
            spatialHashLevel
      );

      double[] projectedUpperLeft = projectToIndexSpace(Point.fromDegrees(upperRight.getLatitude(), lowerLeft.getLongitude()));
      int upperLeftXIndex = getCellIndex(
            projectedUpperLeft[0],
            GeometricConstants.PROJECTED_LONGITUDE_RANGE,
            spatialHashLevel
      );
      int upperLeftYIndex = getCellIndex(
            projectedUpperLeft[1],
            GeometricConstants.PROJECTED_LATITUDE_RANGE,
            spatialHashLevel
      );

      final int startX = Math.min(Math.min(Math.min(lowerLeftXIndex, upperLeftXIndex), upperRightXIndex), lowerRightXIndex);
      final int endX = Math.max(Math.max(Math.max(lowerLeftXIndex, upperLeftXIndex), upperRightXIndex), lowerRightXIndex);

      final int startY = Math.min(Math.min(Math.min(lowerLeftYIndex, upperLeftYIndex), upperRightYIndex), lowerRightYIndex);
      final int endY = Math.max(Math.max(Math.max(lowerLeftYIndex, upperLeftYIndex), upperRightYIndex), lowerRightYIndex);

      List<String> spatialHashCellsIds = new ArrayList<String>((endX + 1 - startX) * (endY + 1 - startY));
      for (int xIndex = startX; xIndex <= endX; xIndex++) {
         for (int yIndex = startY; yIndex <= endY; yIndex++) {
            spatialHashCellsIds.add(formatSpatialHashCellId(xIndex, yIndex));
         }
      }

      return spatialHashCellsIds;
   }

   /**
    * Generate a spatial hash cell ids list for the bounding box of a circular search area
    *
    * @param center           center of the search area
    * @param radius           radius of the search area
    * @param spatialHashLevel spatial hash level of the wanted cell ids
    * @return List of the ids of the cells covering the bounding box of the given search discus
    */
   public static List<String> getSpatialHashCellsIds(Coordinates center, double radius, int spatialHashLevel) {

      Rectangle boundingBox = Rectangle.fromBoundingCircle(center, radius);

      double lowerLeftLatitude = boundingBox.getLowerLeft().getLatitude();
      double lowerLeftLongitude = boundingBox.getLowerLeft().getLongitude();
      double upperRightLatitude = boundingBox.getUpperRight().getLatitude();
      double upperRightLongitude = boundingBox.getUpperRight().getLongitude();

      if (upperRightLongitude < lowerLeftLongitude) { // Box cross the 180 meridian
         final List<String> spatialHashCellsIds;
         spatialHashCellsIds = getSpatialHashCellsIds(
               Point.fromDegreesInclusive(lowerLeftLatitude, lowerLeftLongitude),
               Point.fromDegreesInclusive(upperRightLatitude, GeometricConstants.LONGITUDE_DEGREE_RANGE / 2),
               spatialHashLevel
         );
         spatialHashCellsIds.addAll(
               getSpatialHashCellsIds(
                     Point.fromDegreesInclusive(
                           lowerLeftLatitude,
                           -GeometricConstants.LONGITUDE_DEGREE_RANGE / 2
                     ), Point.fromDegreesInclusive(upperRightLatitude, upperRightLongitude), spatialHashLevel
               )
         );
         return spatialHashCellsIds;
      } else {
         return getSpatialHashCellsIds(
               Point.fromDegreesInclusive(lowerLeftLatitude, lowerLeftLongitude),
               Point.fromDegreesInclusive(upperRightLatitude, upperRightLongitude),
               spatialHashLevel
         );
      }
   }

   /**
    * If point are searched at d distance from a point, a certain spatial hash cell level will problem spatial hash cell
    * that are big enough to contain the search area but the smallest possible. By returning this level we ensure 4
    * spatial hash cell maximum will be needed to cover the search area (2 max on each axis because of search area
    * crossing fixed bonds of the spatial hash cells)
    *
    * @param searchRange search range to be covered by the spatial hash cells
    * @return Return the best spatial hash level for a given search radius.
    */
   public static int findBestSpatialHashLevelForSearchRange(double searchRange) {

      double iterations = GeometricConstants.EARTH_EQUATOR_CIRCUMFERENCE_KM / (2.0d * searchRange);

      return (int) Math.max(0, Math.ceil(Math.log(iterations) / LOG2));
   }

   /**
    * Project a degree latitude/longitude point into a sinusoidal projection planar space for spatial hash cell ids
    * computation
    *
    * @param point point to be projected
    * @return array of projected coordinates
    */
   public static double[] projectToIndexSpace(Point point) {
      double[] projectedCoordinates = new double[2];

      projectedCoordinates[0] = point.getLongitudeRad() * Math.cos(point.getLatitudeRad());
      projectedCoordinates[1] = point.getLatitudeRad();

      return projectedCoordinates;
   }

   public static String formatFieldName(final int spatialHashLevel, final String fieldName) {
      return fieldName + SPATIAL_FIELD_SUFFIX + spatialHashLevel;
   }

   public static String formatLatitude(final String fieldName) {
      return fieldName + SPATIAL_LATITUDE_SUFFIX;
   }

   public static String formatLongitude(final String fieldName) {
      return fieldName + SPATIAL_LONGITUDE_SUFFIX;
   }

   public static boolean isSpatialField(String fieldName) {
      return fieldName.contains(SPATIAL_FIELD_SUFFIX);
   }

   public static boolean isSpatialFieldLatitude(String fieldName) {
      return fieldName.endsWith(SPATIAL_LATITUDE_SUFFIX);
   }

   public static boolean isSpatialFieldLongitude(String fieldName) {
      return fieldName.endsWith(SPATIAL_LONGITUDE_SUFFIX);
   }

   public static String stripSpatialFieldSuffix(String fieldName) {
      if (!isSpatialField(fieldName)) {
         throw new AssertionFailure("The field " + fieldName + " is not a spatial field.");
      }
      return fieldName.substring(0, fieldName.indexOf(SPATIAL_FIELD_SUFFIX));
   }

   public static String formatSpatialHashCellId(final int xIndex, final int yIndex) {
      return xIndex + "|" + yIndex;
   }
}
