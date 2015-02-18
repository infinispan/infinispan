package org.infinispan.commons.configuration.attributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

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
