/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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