package org.infinispan.commons.configuration.attributes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.FileVisitOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.junit.Test;

public class AttributeTest {
   @Test
   public void testAttributeDefinitionType() {
      AttributeDefinition<Long> def = AttributeDefinition.builder("long", 10L).build();
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
      assertNotSame(set1.attribute(def).get(), set2.attribute(def).get());
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

   @Test
   public void testAttributeFromString() {
      AttributeDefinition<Character> charAttrDef = AttributeDefinition.builder("char", 'c').build();
      Attribute<Character> characterAttr = new Attribute<>(charAttrDef);
      characterAttr.fromString("a");
      assertEquals('a', characterAttr.get().charValue());
      assertThatThrownBy(() -> characterAttr.fromString("ab")).isInstanceOf(IllegalArgumentException.class);

      AttributeDefinition<char[]> charArrayAttrDef = AttributeDefinition.builder("chararray", new char[]{}).build();
      Attribute<char[]> charArrayAttr = new Attribute<>(charArrayAttrDef);
      charArrayAttr.fromString("abc");
      assertArrayEquals(new char[]{'a', 'b', 'c'}, charArrayAttr.get());

      AttributeDefinition<Byte> byteAttrDef = AttributeDefinition.builder("byte", (byte) 0).build();
      Attribute<Byte> byteAttr = new Attribute<>(byteAttrDef);
      byteAttr.fromString("5");
      assertEquals(5, byteAttr.get().byteValue());

      AttributeDefinition<Short> shortAttrDef = AttributeDefinition.builder("short", (short) 0).build();
      Attribute<Short> shortAttr = new Attribute<>(shortAttrDef);
      shortAttr.fromString("5");
      assertEquals(5, shortAttr.get().shortValue());

      AttributeDefinition<String> stringAttrDef = AttributeDefinition.builder("string", "").build();
      Attribute<String> stringAttr = new Attribute<>(stringAttrDef);
      stringAttr.fromString("a");
      assertEquals("a", stringAttr.get());

      AttributeDefinition<Integer> integerAttrDef = AttributeDefinition.builder("integer", 0).build();
      Attribute<Integer> integerAttr = new Attribute<>(integerAttrDef);
      integerAttr.fromString("1");
      assertEquals(1, integerAttr.get().intValue());

      AttributeDefinition<Long> longAttrDef = AttributeDefinition.builder("long", 0L).build();
      Attribute<Long> longAttr = new Attribute<>(longAttrDef);
      longAttr.fromString("1");
      assertEquals(1L, longAttr.get().longValue());

      AttributeDefinition<Float> floatAttrDef = AttributeDefinition.builder("float", 0F).build();
      Attribute<Float> floatAttr = new Attribute<>(floatAttrDef);
      floatAttr.fromString("0.1");
      assertEquals(0.1F, floatAttr.get(), 0.000001F);

      AttributeDefinition<Double> doubleAttrDef = AttributeDefinition.builder("double", 0D).build();
      Attribute<Double> doubleAttr = new Attribute<>(doubleAttrDef);
      doubleAttr.fromString("0.1");
      assertEquals(0.1D, doubleAttr.get(), 0.000001D);

      AttributeDefinition<Boolean> booleanAttrDef = AttributeDefinition.builder("boolean", false).build();
      Attribute<Boolean> booleanAttr = new Attribute<>(booleanAttrDef);
      booleanAttr.fromString("true");
      assertTrue(booleanAttr.get());
      booleanAttr.fromString("false");
      assertFalse(booleanAttr.get());
      assertThatThrownBy(() -> booleanAttr.fromString("blah")).isInstanceOf(IllegalArgumentException.class).hasMessage("ISPN000955: 'blah' is not a valid boolean value (true|false|yes|no|y|n|on|off)");

      AttributeDefinition<BigDecimal> bigDecimalAttrDef = AttributeDefinition.builder("bigdecimal", new BigDecimal(0)).build();
      Attribute<BigDecimal> bigDecimalAttr = new Attribute<>(bigDecimalAttrDef);
      bigDecimalAttr.fromString("0.1");
      assertEquals(BigDecimal.valueOf(0.1), bigDecimalAttr.get());

      AttributeDefinition<BigInteger> bigIntegerAttrDef = AttributeDefinition.builder("biginteger", BigInteger.valueOf(0)).build();
      Attribute<BigInteger> bigIntegerAttr = new Attribute<>(bigIntegerAttrDef);
      bigIntegerAttr.fromString("100");
      assertEquals(BigInteger.valueOf(100), bigIntegerAttr.get());

      AttributeDefinition<String[]> stringArrayDef = AttributeDefinition.builder("stringArray", Util.EMPTY_STRING_ARRAY).build();
      Attribute<String[]> stringArray = new Attribute<>(stringArrayDef);
      stringArray.fromString("a b c");
      assertArrayEquals(new String[]{"a", "b", "c"}, stringArray.get());
      stringArray.fromString("d");
      assertArrayEquals(new String[]{"d"}, stringArray.get());
      stringArray.fromString("");
      assertArrayEquals(Util.EMPTY_STRING_ARRAY, stringArray.get());

      AttributeDefinition<List<String>> listAttrDef = AttributeDefinition.builder("list", Collections.emptyList(), (Class<List<String>>) (Class<?>) List.class).build();
      Attribute<List<String>> listAttr = new Attribute<>(listAttrDef);
      listAttr.fromString("a b c");
      assertEquals(Arrays.asList("a", "b", "c"), listAttr.get());
      listAttr.fromString("d");
      assertEquals(List.of("d"), listAttr.get());
      listAttr.fromString("");
      assertEquals(Collections.emptyList(), listAttr.get());

      AttributeDefinition<Set<String>> setAttrDef = AttributeDefinition.builder("set", Collections.emptySet(), (Class<Set<String>>) (Class<?>) Set.class).build();
      Attribute<Set<String>> setAttr = new Attribute<>(setAttrDef);
      setAttr.fromString("a b c");
      assertEquals(Set.of("a", "b", "c"), setAttr.get());
      setAttr.fromString("d");
      assertEquals(Set.of("d"), setAttr.get());
      setAttr.fromString("");
      assertEquals(Collections.emptySet(), setAttr.get());

      AttributeDefinition<File> fileAttrDef = AttributeDefinition.builder("file", null, File.class).build();
      Attribute<File> fileAttr = new Attribute<>(fileAttrDef);
      fileAttr.fromString("/home/user/tmp/test.txt");
      assertEquals(new File("/home/user/tmp/test.txt"), fileAttr.get());

      AttributeDefinition<Properties> propertiesAttrDef = AttributeDefinition.builder("properties", new Properties(), Properties.class).build();
      Attribute<Properties> propertiesAttr = new Attribute<>(propertiesAttrDef);
      propertiesAttr.fromString("A=B\nC=D\n");
      assertThat(propertiesAttr.get()).containsExactlyInAnyOrderEntriesOf(new Properties() {{
         put("A", "B");
         put("C", "D");
      }});

      AttributeDefinition<FileVisitOption> enumAttrDef = AttributeDefinition.builder("enum", null, FileVisitOption.class).build();
      Attribute<FileVisitOption> enumAttr = new Attribute<>(enumAttrDef);
      enumAttr.fromString("FOLLOW_LINKS");
      assertEquals(FileVisitOption.FOLLOW_LINKS, enumAttr.get());
      assertThatThrownBy(() -> enumAttr.fromString("BLAH")).isInstanceOf(IllegalArgumentException.class).hasMessage("ISPN000956: 'BLAH' is not one of [FOLLOW_LINKS]");

      AttributeDefinition<Path> unknownAttrDef = AttributeDefinition.builder("path", null, Path.class).build();
      Attribute<Path> unknownAttr = new Attribute<>(unknownAttrDef);
      assertThatThrownBy(() -> unknownAttr.fromString("a")).isInstanceOf(CacheConfigurationException.class).hasMessageContaining("Cannot convert a to type java.nio.file.Path");
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
