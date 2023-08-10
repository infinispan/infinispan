package org.infinispan.protostream.sampledomain.marshallers;

import org.infinispan.protostream.EnumMarshaller;
import org.infinispan.protostream.sampledomain.User;

/**
 * @author anistor@redhat.com
 * @deprecated This marshalling mechanism will be removed in 5. Please switch to annotation based marshalling.
 */
@Deprecated
public class GenderMarshaller implements EnumMarshaller<User.Gender> {

   @Override
   public Class<User.Gender> getJavaClass() {
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
      return null;  // unknown value
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
