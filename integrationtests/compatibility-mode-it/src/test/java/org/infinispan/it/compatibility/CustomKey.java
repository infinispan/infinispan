package org.infinispan.it.compatibility;

import java.io.Serializable;

public class CustomKey implements Serializable {

   private String text;
   private Double doubleValue;
   private Float floatValue;
   private Boolean booleanValue;

   public CustomKey(String text, Double doubleValue, Float floatValue, Boolean booleanValue) {
      this.text = text;
      this.doubleValue = doubleValue;
      this.floatValue = floatValue;
      this.booleanValue = booleanValue;
   }

   public String getText() {
      return text;
   }

   public void setText(String text) {
      this.text = text;
   }

   public Double getDoubleValue() {
      return doubleValue;
   }

   public void setDoubleValue(Double doubleValue) {
      this.doubleValue = doubleValue;
   }

   public Float getFloatValue() {
      return floatValue;
   }

   public void setFloatValue(Float floatValue) {
      this.floatValue = floatValue;
   }

   public Boolean getBooleanValue() {
      return booleanValue;
   }

   public void setBooleanValue(Boolean booleanValue) {
      this.booleanValue = booleanValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomKey customKey = (CustomKey) o;

      if (!text.equals(customKey.text)) return false;
      if (!doubleValue.equals(customKey.doubleValue)) return false;
      if (!floatValue.equals(customKey.floatValue)) return false;
      return booleanValue.equals(customKey.booleanValue);
   }

   @Override
   public int hashCode() {
      int result = text.hashCode();
      result = 31 * result + doubleValue.hashCode();
      result = 31 * result + floatValue.hashCode();
      result = 31 * result + booleanValue.hashCode();
      return result;
   }
}