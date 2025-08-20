package org.infinispan.query.objectfilter.impl.syntax;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class KnnPredicate implements PrimaryPredicateExpr {

   private final ValueExpr leftChild;
   private final Class<?> expectedType;
   private final List<Object> vector;
   private final ConstantValueExpr.ParamPlaceholder vectorParam;
   private final Object knn;

   public KnnPredicate(ValueExpr leftChild, Class<?> expectedType, List<Object> vector, Object knn) {
      this.leftChild = leftChild;
      this.expectedType = expectedType;
      this.vector = vector;
      this.vectorParam = null;
      this.knn = knn;
   }

   public KnnPredicate(ValueExpr leftChild, Class<?> expectedType, ConstantValueExpr.ParamPlaceholder vector, Object knn) {
      this.leftChild = leftChild;
      this.expectedType = expectedType;
      this.vector = null;
      this.vectorParam = vector;
      this.knn = knn;
   }

   @Override
   public <T> T acceptVisitor(Visitor<?, ?> visitor) {
      return (T) visitor.visit(this);
   }

   @Override
   public ValueExpr getChild() {
      return leftChild;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      appendQueryString(sb);
      return sb.toString();
   }

   @Override
   public void appendQueryString(StringBuilder sb) {
      leftChild.appendQueryString(sb);
      sb.append(" <-> ").append(vector != null ? vector : "[" + vectorParam + "]").append('~').append(knn);
   }

   public Integer knn(Map<String, Object> namedParameters) {
      if (knn == null) {
         return null;
      }
      return convertToInt(knn, namedParameters);
   }

   public boolean floats() {
      return expectedType == float[].class || expectedType == Float.class;
   }

   public byte[] bytesArray(Map<String, Object> namedParameters) {
      if (vector != null) {
         byte[] result = new byte[vector.size()];
         for (int i = 0; i < vector.size(); i++) {
            result[i] = convertToByte(vector.get(i), namedParameters);
         }
         return result;
      }

      String paramName = vectorParam.getName();
      Object value = namedParameters.get(paramName);
      if (value instanceof byte[]) {
         return (byte[]) value;
      }
      throw new IllegalStateException("Wrong parameter type. Param: " + paramName + ". Required type: byte[].");
   }

   public float[] floatsArray(Map<String, Object> namedParameters) {
      if (vector != null) {
         float[] result = new float[vector.size()];
         for (int i = 0; i < vector.size(); i++) {
            result[i] = convertToFloat(vector.get(i), namedParameters);
         }
         return result;
      }

      String paramName = vectorParam.getName();
      Object value = namedParameters.get(paramName);
      if (value instanceof float[]) {
         return (float[]) value;
      }
      throw new IllegalStateException("Wrong parameter type. Param: " + paramName + ". Required type: float[].");
   }

   private int convertToInt(Object valueOrParam, Map<String, Object> namedParameters) {
      return convertValue(valueOrParam, namedParameters,
            (string) -> Integer.parseInt(string), (number) -> number.intValue());
   }

   private byte convertToByte(Object valueOrParam, Map<String, Object> namedParameters) {
      return convertValue(valueOrParam, namedParameters,
            (string) -> Byte.parseByte(string), (number) -> number.byteValue());
   }

   private float convertToFloat(Object valueOrParam, Map<String, Object> namedParameters) {
      return convertValue(valueOrParam, namedParameters,
            (string) -> Float.parseFloat(string), (number) -> number.floatValue());
   }

   private <T> T convertValue(Object valueOrParam, Map<String, Object> namedParameters,
                              Function<String, T> parse, Function<Number, T> convert) {
      if (valueOrParam == null) {
         throw new IllegalStateException("Missing value or parameter");
      }
      Object value = null;
      String paramName = null;

      if (valueOrParam instanceof String) {
         value = valueOrParam;
      }
      if (valueOrParam instanceof ConstantValueExpr.ParamPlaceholder) {
         ConstantValueExpr.ParamPlaceholder param = (ConstantValueExpr.ParamPlaceholder) valueOrParam;
         paramName = param.getName();
         value = namedParameters.get(paramName);
      }

      if (value instanceof Number) {
         return convert.apply((Number) value);
      }
      if (valueOrParam instanceof String) {
         return parse.apply((String) valueOrParam);
      }
      if (value == null) {
         throw new IllegalStateException("Missing value for parameter " + paramName);
      }
      throw new IllegalStateException("Parameter must be a number or a string" + paramName);
   }
}
