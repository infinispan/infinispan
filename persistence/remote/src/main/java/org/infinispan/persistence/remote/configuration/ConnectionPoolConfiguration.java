package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.CONNECTION_POOL;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

@BuiltBy(ConnectionPoolConfigurationBuilder.class)
public class ConnectionPoolConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<ExhaustedAction> EXHAUSTED_ACTION = AttributeDefinition.builder("exhaustedAction", ExhaustedAction.WAIT, ExhaustedAction.class).immutable().build();
   static final AttributeDefinition<Integer> MAX_ACTIVE = AttributeDefinition.builder("maxActive", -1).immutable().build();
   static final AttributeDefinition<Integer> MAX_TOTAL = AttributeDefinition.builder("maxTotal", -1).immutable().build();
   static final AttributeDefinition<Integer> MAX_IDLE = AttributeDefinition.builder("maxIdle", -1).immutable().build();
   static final AttributeDefinition<Integer> MIN_IDLE = AttributeDefinition.builder("minIdle", -1).immutable().build();
   static final AttributeDefinition<Long> TIME_BETWEEN_EVICTION_RUNS = AttributeDefinition.builder("timeBetweenEvictionRuns", 120000L).immutable().build();
   static final AttributeDefinition<Long> MIN_EVICTABLE_IDLE_TIME = AttributeDefinition.builder("minEvictableIdleTime", 1800000L).immutable().build();
   static final AttributeDefinition<Boolean> TEST_WHILE_IDLE = AttributeDefinition.builder("testWhileIdle", true).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ConnectionPoolConfiguration.class, EXHAUSTED_ACTION, MAX_ACTIVE, MAX_TOTAL, MAX_IDLE,
            MIN_IDLE, TIME_BETWEEN_EVICTION_RUNS, MIN_EVICTABLE_IDLE_TIME, TEST_WHILE_IDLE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CONNECTION_POOL.getLocalName());

   private final AttributeSet attributes;

   ConnectionPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }


   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ExhaustedAction exhaustedAction() {
      return attributes.attribute(EXHAUSTED_ACTION).get();
   }

   public int maxActive() {
      return attributes.attribute(MAX_ACTIVE).get();
   }

   public int maxTotal() {
      return attributes.attribute(MAX_TOTAL).get();
   }

   public int maxIdle() {
      return attributes.attribute(MAX_IDLE).get();
   }

   public int minIdle() {
      return attributes.attribute(MIN_IDLE).get();
   }

   public long timeBetweenEvictionRuns() {
      return attributes.attribute(TIME_BETWEEN_EVICTION_RUNS).get();
   }

   public long minEvictableIdleTime() {
      return attributes.attribute(MIN_EVICTABLE_IDLE_TIME).get();
   }

   public boolean testWhileIdle() {
      return attributes.attribute(TEST_WHILE_IDLE).get();
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [exhaustedAction=" + exhaustedAction() + ", maxActive=" + maxActive()
            + ", maxTotal=" + maxTotal() + ", maxIdle=" + maxIdle() + ", minIdle=" + minIdle() + ", timeBetweenEvictionRuns="
            + timeBetweenEvictionRuns() + ", minEvictableIdleTime=" + minEvictableIdleTime() + ", testWhileIdle="
            + testWhileIdle() + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConnectionPoolConfiguration that = (ConnectionPoolConfiguration) o;

      if (maxActive() != that.maxActive()) return false;
      if (maxTotal() != that.maxTotal()) return false;
      if (maxIdle() != that.maxIdle()) return false;
      if (minIdle() != that.minIdle()) return false;
      if (timeBetweenEvictionRuns() != that.timeBetweenEvictionRuns()) return false;
      if (minEvictableIdleTime() != that.minEvictableIdleTime()) return false;
      if (testWhileIdle() != that.testWhileIdle()) return false;
      return exhaustedAction() == that.exhaustedAction();

   }

   @Override
   public int hashCode() {
      int result = exhaustedAction() != null ? exhaustedAction().hashCode() : 0;
      result = 31 * result + maxActive();
      result = 31 * result + maxTotal();
      result = 31 * result + maxIdle();
      result = 31 * result + minIdle();
      result = 31 * result + (int) (timeBetweenEvictionRuns() ^ (timeBetweenEvictionRuns() >>> 32));
      result = 31 * result + (int) (minEvictableIdleTime() ^ (minEvictableIdleTime() >>> 32));
      result = 31 * result + (testWhileIdle() ? 1 : 0);
      return result;
   }
}
