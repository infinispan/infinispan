package org.infinispan.commons.util;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

/**
 * Test for {@link TypedProperties}.
 *
 * @author Diego Lovison
 * @since 12.1
 **/
public class TypedPropertiesTest {

   @Test
   public void testIntProperty() {
      TypedProperties p = createProperties();
      assertEquals(1, p.getIntProperty("int", 999));
      assertEquals(2, p.getIntProperty("int_as_long", 999));
      assertEquals(3, p.getIntProperty("int_as_short", 999));
      assertEquals(10, p.getIntProperty("int_put_str", 999));
      assertEquals(1, p.getIntProperty("int_invalid", 1));
      assertEquals(1, p.getIntProperty("int_null", 1));

      System.setProperty("fooVar", "100");
      assertEquals(100, p.getIntProperty("int_key_value_replacement", 1, true));
      System.clearProperty("fooVar");
   }

   @Test
   public void testLongProperty() {
      TypedProperties p = createProperties();
      assertEquals(2L, p.getLongProperty("long", 999L));
      assertEquals(3L, p.getLongProperty("long_as_int", 999L));
      assertEquals(4L, p.getLongProperty("long_as_short", 999L));
      assertEquals(20L, p.getLongProperty("long_put_str", 999L));
      assertEquals(2L, p.getLongProperty("long_invalid", 2L));
      assertEquals(2L, p.getLongProperty("long_null", 2L));
   }

   @Test
   public void testBooleanProperty() {
      TypedProperties p = createProperties();
      assertEquals(true, p.getBooleanProperty("boolean", false));
      assertEquals(true, p.getBooleanProperty("boolean_put_str", false));
      assertEquals(true, p.getBooleanProperty("boolean_invalid", true));
      assertEquals(true, p.getBooleanProperty("boolean_null", true));
   }

   @Test
   public void testDurationProperty() {
      TypedProperties p = createProperties();
      assertEquals(10, p.getDurationProperty("duration_int", 42));
      assertEquals(20, p.getDurationProperty("duration_long", 42));
      assertEquals(30, p.getDurationProperty("duration_short", 42));
      assertEquals(100, p.getDurationProperty("duration_srt", 42));
      assertEquals(42, p.getDurationProperty("duration_invalid", 42));
   }

   @Test
   public void testEnumProperty() {
      TypedProperties p = createProperties();
      assertEquals(COLOR.BLUE, p.getEnumProperty("enum_cast", COLOR.class, COLOR.BLUE));
      assertEquals(COLOR.RED, p.getEnumProperty("enum", COLOR.class, COLOR.BLUE));
      assertEquals(COLOR.RED, p.getEnumProperty("enum_put_str", COLOR.class, COLOR.BLUE));
      assertEquals(COLOR.BLUE, p.getEnumProperty("enum_invalid", COLOR.class, COLOR.BLUE));
      assertEquals(COLOR.BLUE, p.getEnumProperty("enum_null", COLOR.class, COLOR.BLUE));
      assertEquals(COLOR.BLUE, p.getEnumProperty("enum_other", COLOR.class, COLOR.BLUE));
   }

   private enum COLOR {
      RED, BLUE
   }

   private enum NUMBER {
      NUMBER_1
   }

   private TypedProperties createProperties() {
      Properties p = new Properties();
      p.put("int", 1);
      p.put("int_as_long", Long.valueOf("2"));
      p.put("int_as_short", Short.valueOf("3"));
      p.put("int_put_str", Integer.toString(10));
      p.put("int_invalid", false);
      p.put("int_key_value_replacement", "${fooVar}");

      p.put("long", 2L);
      p.put("long_as_int", Integer.valueOf("3"));
      p.put("long_as_short", Short.valueOf("4"));
      p.put("long_put_str", Long.toString(20L));
      p.put("long_invalid", false);

      p.put("boolean", true);
      p.put("boolean_put_str", Boolean.toString(true));
      p.put("boolean_invalid", COLOR.RED);

      p.put("enum", COLOR.RED);
      p.put("enum_put_str", COLOR.RED.toString());
      p.put("enum_invalid", false);
      p.put("enum_other", NUMBER.NUMBER_1);

      p.put("duration_int", Integer.valueOf("10"));
      p.put("duration_long", Long.valueOf("20"));
      p.put("duration_short", Short.valueOf("30"));
      p.put("duration_srt", "100");
      p.put("duration_invalid", false);

      return new TypedProperties(p);
   }
}
