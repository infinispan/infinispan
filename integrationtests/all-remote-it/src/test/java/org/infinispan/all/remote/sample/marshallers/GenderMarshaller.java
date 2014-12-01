package org.infinispan.all.remote.sample.marshallers;

import org.infinispan.all.remote.sample.classes.User;
import org.infinispan.protostream.EnumMarshaller;

/**
 * @author anistor@redhat.com
 */
public class GenderMarshaller implements EnumMarshaller<User.Gender> {
   @Override
   public Class<? extends User.Gender> getJavaClass() {
      return User.Gender.class;
   }
   @Override
   public String getTypeName() {
      return "sample_bank_account.User.Gender";
   }
   @Override
   public User.Gender decode(int enumValue) {
      switch (enumValue) {
         case 0:
            return User.Gender.MALE;
         case 1:
            return User.Gender.FEMALE;
      }
      return null; // unknown value
   }
   @Override
   public int encode(User.Gender gender) {
      switch (gender) {
         case MALE:
            return 0;
         case FEMALE:
            return 1;
         default:
            throw new IllegalArgumentException("Unexpected User.Gender value : " + gender);
      }
   }
}
