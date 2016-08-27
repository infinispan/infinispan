package org.infinispan.atomic;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class TestDeltaAware implements DeltaAware, Serializable {

   private String firstComponent;
   private String secondComponent;

   private transient TestDelta delta;

   @Override
   public void commit() {
      delta = null;
   }

   @Override
   public Delta delta() {
      Delta toReturn = getDelta();
      delta = null;
      return toReturn;
   }

   private TestDelta getDelta() {
      if (delta == null)
         delta = new TestDelta();
      return delta;
   }

   public String getFirstComponent() {
      return firstComponent;
   }

   public void setFirstComponent(String firstComponent) {
      getDelta().registerComponentChange("firstComponent", firstComponent);
      this.firstComponent = firstComponent;
   }

   public String getSecondComponent() {
      return secondComponent;
   }

   public void setSecondComponent(String secondComponent) {
      getDelta().registerComponentChange("secondComponent", secondComponent);
      this.secondComponent = secondComponent;
   }

   static class TestDelta implements Delta, Serializable {

      private final HashMap<String, String> changeLog = new HashMap<String, String>();

      void registerComponentChange(String componentName, String componentValue) {
         changeLog.put(componentName, componentValue);
      }

      @Override
      public DeltaAware merge(DeltaAware d) {
         TestDeltaAware other = d instanceof TestDeltaAware ? (TestDeltaAware) d : new TestDeltaAware();
         for (String componentName : changeLog.keySet()) {
            String componentValue = changeLog.get(componentName);
            if (componentName.equals("firstComponent")) {
               other.firstComponent = componentValue;
            } else if (componentName.equals("secondComponent")) {
               other.secondComponent = componentValue;
            } else {
               throw new RuntimeException("Unknown component: " + componentName);
            }
         }
         return other;
      }
   }
}
