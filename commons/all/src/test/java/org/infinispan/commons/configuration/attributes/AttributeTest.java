package org.infinispan.commons.configuration.attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.util.TypedProperties;
import org.junit.Test;

public class AttributeTest {
   @Test
   public void testAttributeDefinitionType() {
      AttributeDefinition<Long> def = AttributeDefinition.builder("long", 10l).build();
      assertEquals(Long.class, def.getType());

      AttributeDefinition<String> def2 = AttributeDefinition.builder("string", null, String.class).build();
      assertEquals(String.class, def2.getType());
   }

   @Test
   public void testAttributeSetProtection() {
      AttributeDefinition<String> immutable = AttributeDefinition.builder("immutable", "").immutable().build();
      AttributeDefinition<String> mutable = AttributeDefinition.builder("mutable", "").build();
      AttributeSet set = new AttributeSet(getClass(), immutable, mutable);
      set.attribute(immutable).set("an immutable string");
      set.attribute(mutable).set("a mutable string");
      set = set.protect();
      try {
         set.attribute(immutable).set("this should fail");
         fail("Changing an immutable attribute on a protected set should fail");
      } catch (Exception e) {
         // Expected
      }
      set.attribute(mutable).set("this should work");
   }

   @Test
   public void testAttributeChanges() {
      AttributeDefinition<Boolean> def = AttributeDefinition.builder("test", false).build();
      Attribute<Boolean> attribute = def.toAttribute();
      assertFalse(attribute.isModified());
      attribute.set(true);
      assertEquals(true, attribute.get());
      assertTrue(attribute.isModified());
   }

   @Test
   public void testAttributeInitializer() {
      AttributeDefinition<Properties> def = AttributeDefinition.builder("props", null, Properties.class).initializer(Properties::new).build();
      Attribute<Properties> attribute1 = def.toAttribute();
      Attribute<Properties> attribute2 = def.toAttribute();
      assertNotSame(attribute1.get(), attribute2.get());
   }

   @Test
   public void testDefaultAttributeCopy() {
      AttributeDefinition<Boolean> def = AttributeDefinition.builder("test", false).build();
      AttributeSet set1 = new AttributeSet("set", def);
      set1.attribute(def).set(true);
      AttributeSet set2 = new AttributeSet("set", def);
      set2.read(set1, Combine.DEFAULT);
      assertEquals(set1.attribute(def).get(), set2.attribute(def).get());
   }

   @Test
   public void testAttributeOverride() {
      AttributeDefinition<Boolean> one = AttributeDefinition.builder("one", false).build();
      AttributeDefinition<Boolean> two = AttributeDefinition.builder("two", false).build();
      AttributeSet set1 = new AttributeSet("set", one, two);
      set1.attribute(one).set(true);
      AttributeSet set2 = new AttributeSet("set", one, two);
      set2.attribute(two).set(true);
      set2.read(set1, new Combine(Combine.RepeatedAttributes.OVERRIDE, Combine.Attributes.OVERRIDE));
      assertEquals(set1.attribute(one).get(), set2.attribute(one).get());
      assertFalse(set2.attribute(two).isModified());
   }

   @Test
   public void testCollectionAttributeCopy() {
      AttributeDefinition<TypedProperties> def = AttributeDefinition.builder("properties", null, TypedProperties.class).copier(TypedPropertiesAttributeCopier.INSTANCE).initializer(TypedProperties::new).build();
      AttributeSet set1 = new AttributeSet("set", def);
      TypedProperties typedProperties = set1.attribute(def).get();
      typedProperties.setProperty("key", "value");
      set1.attribute(def).set(typedProperties);
      set1 = set1.protect();
      typedProperties = set1.attribute(def).get();
      typedProperties.setProperty("key", "anotherValue");
      AttributeSet set2 = new AttributeSet("set", def);
      set2.read(set1, Combine.DEFAULT);
   }

   @Test
   public void testCustomAttributeCopier() {
      AttributeDefinition<List<String>> def = AttributeDefinition.builder("test", Arrays.asList("a", "b")).copier(attribute -> {
         if (attribute == null)
            return null;
         else
            return new ArrayList<>(attribute);
      }).build();
      AttributeSet set1 = new AttributeSet("set", def);
      set1.attribute(def).set(Arrays.asList("b", "a"));
      AttributeSet set2 = new AttributeSet("set", def);
      set2.read(set1, Combine.DEFAULT);
      assertEquals(set1.attribute(def).get(), set2.attribute(def).get());
      assertFalse(set1.attribute(def).get() == set2.attribute(def).get());
   }

   @Test
   public void testAttributeListener() {
      AttributeDefinition<Boolean> def = AttributeDefinition.builder("test", false).build();
      Attribute<Boolean> attribute = def.toAttribute();
      final Holder<Boolean> listenerInvoked = new Holder<>(false);
      attribute.addListener((attribute1, oldValue) -> {
         assertTrue(attribute1.get());
         assertFalse(oldValue);
         listenerInvoked.set(true);
      });
      attribute.set(true);
      assertTrue("Attribute listener was not invoked", listenerInvoked.get());
   }

   @Test
   public void testAttributeSetMatches() {
      AttributeDefinition<String> local = AttributeDefinition.builder("local", "").global(false).build();
      AttributeDefinition<String> global = AttributeDefinition.builder("global", "").build();
      AttributeSet setA = new AttributeSet(getClass(), local, global);
      AttributeSet setB = new AttributeSet(getClass(), local, global);
      setA.attribute("local").set("A");
      setA.attribute("global").set("A");
      setB.attribute("local").set("B");
      setB.attribute("global").set("A");
      assertTrue(setA.matches(setB));
      assertNotEquals(setA, setB);
      setB.attribute("global").set("B");
      assertFalse(setA.matches(setB));
      assertNotEquals(setA, setB);

      setA = new AttributeSet(getClass(), local);
      setB = new AttributeSet(getClass(), local, global);
      setA.attribute("local").set("A");
      setB.attribute("local").set("A");
      setB.attribute("global").set("A");
      assertFalse(setA.matches(setB));
      assertNotEquals(setA, setB);

      AttributeDefinition<String> global2 = AttributeDefinition.builder("global2", "").build();
      setA = new AttributeSet(getClass(), local, global);
      setB = new AttributeSet(getClass(), local, global2);
      setA.attribute("local").set("A");
      setA.attribute("global").set("A");
      setB.attribute("local").set("A");
      setB.attribute("global2").set("A");
      assertFalse(setA.matches(setB));
      assertNotEquals(setA, setB);
   }

   static class Holder<T> {
      T object;

      Holder(T object) {
         this.object = object;
      }

      Holder() {
         this.object = null;
      }

      T get() {
         return object;
      }

      void set(T object) {
         this.object = object;
      }
   }

}
