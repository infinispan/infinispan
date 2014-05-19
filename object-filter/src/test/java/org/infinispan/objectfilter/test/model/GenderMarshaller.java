package org.infinispan.objectfilter.test.model;

import org.infinispan.protostream.EnumMarshaller;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class GenderMarshaller implements EnumMarshaller<Person.Gender> {

   @Override
   public Person.Gender decode(int enumValue) {
      switch (enumValue) {
         case 0:
            return Person.Gender.MALE;
         case 1:
            return Person.Gender.FEMALE;
      }
      return null;  // unknown value
   }

   @Override
   public int encode(Person.Gender gender) {
      switch (gender) {
         case MALE:
            return 0;
         case FEMALE:
            return 1;
         default:
            throw new IllegalArgumentException("Unexpected User.Gender value : " + gender);
      }
   }

   @Override
   public Class<? extends Person.Gender> getJavaClass() {
      return Person.Gender.class;
   }

   @Override
   public String getTypeName() {
      return "org.infinispan.objectfilter.test.model.Person.Gender";
   }
}
