package org.infinispan.remoting;

import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A helper to perform common arithmetic functions with membership lists
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MembershipArithmetic {
   public static List<Address> getMembersJoined(List<Address> oldList, List<Address> newList) {
      Set<Address> tmp = new HashSet<Address>(newList);
      tmp.removeAll(oldList);
      return new ArrayList<Address>(tmp);
   }
   
   public static List<Address> getMembersLeft(List<Address> oldList, List<Address> newList) {
      Set<Address> tmp = new HashSet<Address>(oldList);
      tmp.removeAll(newList);
      return new ArrayList<Address>(tmp);
   }

   public static Address getMemberJoined(List<Address> oldList, List<Address> newList) {
      Set<Address> tmp = new HashSet<Address>(newList);
      tmp.removeAll(oldList);
      return tmp.isEmpty() ? null : tmp.iterator().next();
   }

   public static Address getMemberLeft(List<Address> oldList, List<Address> newList) {
      Set<Address> tmp = new HashSet<Address>(oldList);
      tmp.removeAll(newList);
      return tmp.isEmpty() ? null : tmp.iterator().next();
   }   
}
