/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package org.infinispan.marshall.multiversion;

import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AbstractDelegatingMarshaller;
import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.SerializeWith;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CherryPickClassLoader;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.Util;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Test how marshalling code can deal with new versions of classes being used
 * to marshall/unmarshall old versions. This includes tests for situations
 * where fields are added and when fields are removed.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.multiversion.MultiPojoVersionMarshallTest")
public class MultiPojoVersionMarshallTest extends AbstractInfinispanTest {

   private static final String BASE = "org.infinispan.marshall.multiversion.MultiPojoVersionMarshallTest$";
   private static final String CAR = BASE + "Car";
   private static final String CAR_EXT = CAR + "Externalizer";
   private static final String PERSON = BASE + "Person";
   private static final String PERSON_EXT = PERSON + "Externalizer";
   private static final String HOUSE = BASE + "House";
   private static final String HOUSE_EXT = HOUSE + "Externalizer";

   private AbstractDelegatingMarshaller marshaller;
   private EmbeddedCacheManager cm;

   @BeforeTest
   public void setUp() {
      cm = TestCacheManagerFactory.createLocalCacheManager(false);
      marshaller = extractCacheMarshaller(cm.getCache());
   }

   @AfterTest
   public void tearDown() {
      if (cm != null) cm.stop();
   }

   public void testAddIntFieldDiffIspnExternalizer() throws Exception {
      MarshallingMethod method = MarshallingMethod.INFINISPAN;
      byte[] oldCarbytes = marshallOldCar(method);
      readOldCarWithNewVersion(oldCarbytes, method, true);
   }

   public void testAddStringFieldDiffIspnExternalizer() throws Exception {
      MarshallingMethod method = MarshallingMethod.INFINISPAN;
      byte[] bytes = marshallOldPerson(method);
      readOldPersonWithNewVersion(bytes, method, true);
   }

   public void testRemoveFieldIspnExternalizer() throws Exception {
      MarshallingMethod method = MarshallingMethod.INFINISPAN;
      byte[] bytes = marshallOldHouse(method);
      readOldHouseWithNewVersion(bytes, method, true);
   }

   private byte[] marshallOldHouse(MarshallingMethod method) throws Exception {
      Class clazz = Util.loadClass(HOUSE, Thread.currentThread().getContextClassLoader());
      Object old = clazz.newInstance();
      Field street = clazz.getDeclaredField("street");
      street.set(old, "Rue du Seyon");
      Field number = clazz.getDeclaredField("number");
      number.set(old, 73);
      return marshall(old, method);

   }

   private byte[] marshallOldCar(MarshallingMethod method) throws Exception {
      Class oldCarClass = Util.loadClass(CAR, Thread.currentThread().getContextClassLoader());
      Object oldCar = oldCarClass.newInstance();
      Field oldPlate = oldCarClass.getDeclaredField("plateNumber");
      oldPlate.set(oldCar, "E 1660");
      return marshall(oldCar, method);
   }

   private byte[] marshallOldPerson(MarshallingMethod method) throws Exception {
      Class clazz = Util.loadClass(PERSON, Thread.currentThread().getContextClassLoader());
      Object old = clazz.newInstance();
      Field ageField = clazz.getDeclaredField("age");
      ageField.set(old, 23);
      return marshall(old, method);
   }

   private void readOldCarWithNewVersion(byte[] oldCarbytes,
         MarshallingMethod method, boolean isNewExternalizer) throws Exception {
      // Set up a different classloader to load a different version of the class
      String[] included = new String[]{CAR, CAR_EXT};
      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      ClassLoader cherryPickCl = new CherryPickClassLoader(included, null, tccl);
      Thread.currentThread().setContextClassLoader(cherryPickCl);
      ClassPool pool = ClassPool.getDefault();
      // Insert a classpath so that Maven does not complain about not finding the class
      pool.insertClassPath(new ClassClassPath(Car.class));
      CtClass carCt = pool.get(CAR);
      try {
         carCt.addField(CtField.make("public int year;", carCt));
         Class carClass = carCt.toClass();
         if (isNewExternalizer) {
            CtClass carExtCt = pool.get(CAR_EXT);
            CtMethod writeObjMeth = carExtCt.getMethod("writeObject",
                  "(Ljava/io/ObjectOutput;Ljava/lang/Object;)V");
            writeObjMeth.setBody("{\n" +
               "$1.writeObject(((" + CAR + ") $2).plateNumber);\n" +
               "$1.writeInt(((" + CAR + ") $2).year);\n" +
            "}\n"
            );
            CtMethod readObjMeth = carExtCt.getMethod("readObject",
                  "(Ljava/io/ObjectInput;)Ljava/lang/Object;");
            readObjMeth.setBody("{\n" +
               CAR + " o = new " + CAR + "();\n" +
               "o.plateNumber = (String) $1.readObject();\n" +
               "int b1 = $1.read();\n" +
               "System.out.println(b1);\n" +
               "if (b1 != -1) {\n" + // Check whether end of stream has been reached.
               "   byte b2 = $1.readByte();\n" +
               "   byte b3 = $1.readByte();\n" +
               "   byte b4 = $1.readByte();\n" +
               "   o.year = ((0xFF & b1) << 24) | ((0xFF & b2) << 16) |\n" +
               "            ((0xFF & b3) << 8) | (0xFF & b4);\n" +
               "}\n" +
               "return o;\n" +
            "}\n"
            );
            carExtCt.toClass(); // Convert to class so that it gets loaded!!!
         }

         Object oldCarFromWire = unmarshall(oldCarbytes, method);

         Field plateField = carClass.getDeclaredField("plateNumber");
         assertEquals("E 1660", plateField.get(oldCarFromWire));
         // The payload was read but no year populated, so default.
         Field yearField = carClass.getDeclaredField("year");
         assertEquals(0, yearField.get(oldCarFromWire));

         if (isNewExternalizer) {
            // Now try to create a new instance of the new class and marshall/unmarshall it
            Object newCar = carClass.newInstance();
            plateField = carClass.getDeclaredField("plateNumber");
            plateField.set(newCar, "CH 8271");
            yearField = carClass.getDeclaredField("year");
            yearField.set(newCar, 2001);
            byte[] bytes = marshall(newCar, method);
            Object readNewCar = unmarshall(bytes, method);
            plateField = carClass.getDeclaredField("plateNumber");
            assertEquals("CH 8271", plateField.get(readNewCar));
            yearField = carClass.getDeclaredField("year");
            assertEquals(2001, yearField.get(readNewCar));
         }
      } finally {
         carCt.detach();
         Thread.currentThread().setContextClassLoader(tccl);
      }
   }

   private void readOldPersonWithNewVersion(byte[] bytes,
         MarshallingMethod method, boolean isNewExternalizer) throws Exception {
      // Set up a different classloader to load a different version of the class
      String[] included = new String[]{PERSON, PERSON_EXT};
      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      ClassLoader cherryPickCl = new CherryPickClassLoader(included, null, tccl);
      Thread.currentThread().setContextClassLoader(cherryPickCl);
      ClassPool pool = ClassPool.getDefault();
      CtClass ct = pool.get(PERSON);
      try {
         ct.addField(CtField.make("public String name;", ct));
         Class clazz = ct.toClass();
         if (isNewExternalizer) {
            CtClass extCt = pool.get(PERSON_EXT);
            CtMethod writeObjMeth = extCt.getMethod("writeObject", "(Ljava/io/ObjectOutput;Ljava/lang/Object;)V");
            writeObjMeth.setBody("{\n" +
               "$1.writeInt(((" + PERSON + ") $2).age);\n" +
               "$1.writeObject((("  + PERSON + ") $2).name);\n" +
            "}\n"
            );
            CtMethod readObjMeth = extCt.getMethod("readObject", "(Ljava/io/ObjectInput;)Ljava/lang/Object;");
            readObjMeth.setBody("{\n" +
               PERSON + " o = new " + PERSON + "();\n" +
               "o.age = $1.readInt();\n" +
               "try {\n" +
               "   o.name = (String) $1.readObject();\n" +
               "} catch(java.io.OptionalDataException e) {}\n" +
               "return o;\n" +
            "}\n"
            );
            extCt.toClass(); // Convert to class so that it gets loaded!!!
         }

         Object oldFromWire = unmarshall(bytes, method);

         Field age = clazz.getDeclaredField("age");
         assertEquals(23, age.get(oldFromWire));
         // The payload was read but no year populated, so default.
         Field name = clazz.getDeclaredField("name");
         assertNull(name.get(oldFromWire));

         if (isNewExternalizer) {
            // Now try to create a new instance of the new class and marshall/unmarshall it
            Object newObj = clazz.newInstance();
            age = clazz.getDeclaredField("age");
            age.set(newObj, 34);
            name = clazz.getDeclaredField("name");
            name.set(newObj, "Galder");
            bytes = marshall(newObj, method);
            Object newFromWire = unmarshall(bytes, method);
            age = clazz.getDeclaredField("age");
            assertEquals(34, age.get(newFromWire));
            name = clazz.getDeclaredField("name");
            assertEquals("Galder", name.get(newFromWire));
         }
      } finally {
         ct.detach();
         Thread.currentThread().setContextClassLoader(tccl);
      }
   }

   private void readOldHouseWithNewVersion(byte[] bytes,
         MarshallingMethod method, boolean isNewExternalizer) throws Exception {
      // Set up a different classloader to load a different version of the class
      String[] included = new String[]{HOUSE, HOUSE_EXT};
      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      ClassLoader cherryPickCl = new CherryPickClassLoader(included, null, tccl);
      Thread.currentThread().setContextClassLoader(cherryPickCl);
      ClassPool pool = ClassPool.getDefault();
      CtClass ct = pool.get(HOUSE);
      try {
         ct.removeField(ct.getField("number"));
         Class clazz = ct.toClass();
         if (isNewExternalizer) {
            CtClass extCt = pool.get(HOUSE_EXT);
            CtMethod writeObjMeth = extCt.getMethod("writeObject", "(Ljava/io/ObjectOutput;Ljava/lang/Object;)V");
            writeObjMeth.setBody("{\n" +
               "$1.writeInt(0);\n" + // Safe the spot to avoid incompatibility
               "$1.writeObject((("  + HOUSE + ") $2).street);\n" +
            "}\n"
            );
            CtMethod readObjMeth = extCt.getMethod("readObject", "(Ljava/io/ObjectInput;)Ljava/lang/Object;");
            readObjMeth.setBody("{\n" +
               HOUSE + " o = new " + HOUSE + "();\n" +
               "try {\n" +
               "   $1.readInt();\n" +
               "} catch(java.io.OptionalDataException e) {}\n" +
               "o.street = (String) $1.readObject();\n" +
               "return o;\n" +
            "}\n"
            );
            extCt.toClass(); // Convert to class so that it gets loaded!!!
         }

         Object oldFromWire = unmarshall(bytes, method);

         Field street = clazz.getDeclaredField("street");
         assertEquals("Rue du Seyon", street.get(oldFromWire));

         if (isNewExternalizer) {
            // Now try to create a new instance of the new class and marshall/unmarshall it
            Object newObj = clazz.newInstance();
            street = clazz.getDeclaredField("street");
            street.set(newObj, "Fir Close");
            bytes = marshall(newObj, method);
            Object newFromWire = unmarshall(bytes, method);
            street = clazz.getDeclaredField("street");
            assertEquals("Fir Close", street.get(newFromWire));
         }
      } finally {
         ct.detach();
         Thread.currentThread().setContextClassLoader(tccl);
      }
   }

   private byte[] marshall(Object o, MarshallingMethod method) throws Exception {
      switch (method) {
         case INFINISPAN:
         case JBOSS_MARSHALLING:
            return getMarshaller().objectToByteBuffer(o);
         case JAVA: {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            try {
               oos.writeObject(o);
               return baos.toByteArray();
            } finally {
               oos.close();
               baos.close();
            }
         }
         default: {
            return null;
         }
      }
   }

   private Object unmarshall(byte[] bytes, MarshallingMethod method) throws Exception {
      switch (method) {
         case INFINISPAN:
         case JBOSS_MARSHALLING: {
            return getMarshaller().objectFromByteBuffer(bytes);
         }
         case JAVA: {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            try {
               return ois.readObject();
            } finally {
               bais.close();
               ois.close();
            }
         }
         default: {
            return null;
         }
      }
   }

   private AbstractDelegatingMarshaller getMarshaller() {
      return marshaller;
   }

   @SerializeWith(CarExternalizer.class)
   public static class Car {
      public String plateNumber;
   }

   public static class CarExternalizer implements Externalizer<Car>, Serializable {
      @Override
      public void writeObject(ObjectOutput output, Car object) throws IOException {
         output.writeObject(object.plateNumber);
      }

      @Override
      public Car readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Car o = new Car();
         o.plateNumber = (String) input.readObject();
         return o;
      }
   }

   @SerializeWith(PersonExternalizer.class)
   public static class Person {
      public int age;
   }

   public static class PersonExternalizer implements Externalizer<Person>, Serializable {
      @Override
      public void writeObject(ObjectOutput output, Person object) throws IOException {
         output.writeInt(object.age);
      }

      @Override
      public Person readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Person o = new Person();
         o.age = input.readInt();
         return o;
      }
   }

   @SerializeWith(HouseExternalizer.class)
   public static class House {
      public String street;
      public int number;
   }

   public static class HouseExternalizer implements Externalizer<House>, Serializable {
      @Override
      public void writeObject(ObjectOutput output, House object) throws IOException {
         output.writeInt(object.number);
         output.writeObject(object.street);
      }

      @Override
      public House readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         House o = new House();
         o.number = input.readInt();
         o.street = (String) input.readObject();
         return o;
      }
   }

   public static enum MarshallingMethod {
      INFINISPAN,
      JAVA,
      JBOSS_MARSHALLING
   }

}
