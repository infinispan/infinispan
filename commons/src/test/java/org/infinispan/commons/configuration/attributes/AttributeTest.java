package org.infinispan.commons.configuration.attributes;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.util.TypedProperties;
import org.junit.Ignore;
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
      AttributeDefinition<Properties> def = AttributeDefinition.builder("props", null, Properties.class).initializer(new AttributeInitializer<Properties>() {

         @Override
         public Properties initialize() {
            return new Properties();
         }
      }).build();
      Attribute<Properties> attribute1 = def.toAttribute();
      Attribute<Properties> attribute2 = def.toAttribute();
      assertTrue(attribute1.get() != attribute2.get());
   }

   @Test
   public void testDefaultAttributeCopy() {
      AttributeDefinition<Boolean> def = AttributeDefinition.builder("test", false).build();
      AttributeSet set1 = new AttributeSet("set", def);
      set1.attribute(def).set(true);
      AttributeSet set2 = new AttributeSet("set", def);
      set2.read(set1);
      assertEquals(set1.attribute(def).get(), set2.attribute(def).get());
   }


   @Test
   public void testCollectionAttributeCopy() {
      AttributeDefinition<TypedProperties> def = AttributeDefinition.builder("properties", null, TypedProperties.class).copier(TypedPropertiesAttributeCopier.INSTANCE).initializer(new AttributeInitializer<TypedProperties>() {
         @Override
         public TypedProperties initialize() {
            return new TypedProperties();
         }
      }).build();
      AttributeSet set1 = new AttributeSet("set", def);
      TypedProperties typedProperties = set1.attribute(def).get();
      typedProperties.setProperty("key", "value");
      set1.attribute(def).set(typedProperties);
      set1 = set1.protect();
      typedProperties = set1.attribute(def).get();
      typedProperties.setProperty("key", "anotherValue");
      AttributeSet set2 = new AttributeSet("set", def);
      set2.read(set1);
   }

   @Test
   public void testCustomAttributeCopier() {
      AttributeDefinition<List<String>> def = AttributeDefinition.builder("test", Arrays.asList("a", "b")).copier(new AttributeCopier<List<String>>() {
         @Override
         public List<String> copyAttribute(List<String> attribute) {
            if (attribute == null)
               return null;
            else
               return new ArrayList(attribute);
         }
      }).build();
      AttributeSet set1 = new AttributeSet("set", def);
      set1.attribute(def).set(Arrays.asList("b", "a"));
      AttributeSet set2 = new AttributeSet("set", def);
      set2.read(set1);
      assertEquals(set1.attribute(def).get(), set2.attribute(def).get());
      assertFalse(set1.attribute(def).get() == set2.attribute(def).get());
   }

   @Test
   public void testAttributeListener() {
      AttributeDefinition<Boolean> def = AttributeDefinition.builder("test", false).build();
      Attribute<Boolean> attribute = def.toAttribute();
      final Holder<Boolean> listenerInvoked = new Holder<>(false);
      attribute.addListener(new AttributeListener<Boolean>() {

         @Override
         public void attributeChanged(Attribute<Boolean> attribute, Boolean oldValue) {
            assertTrue(attribute.get());
            assertFalse(oldValue);
            listenerInvoked.set(true);
         }
      });
      attribute.set(true);
      assertTrue("Attribute listener was not invoked", listenerInvoked.get());
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
