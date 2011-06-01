/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

   public static Set<Address> getMembersJoined(Set<Address> oldSet, Set<Address> newSet) {
      Set<Address> result = new HashSet<Address>(newSet);
      result.removeAll(oldSet);
      return result;
   }

   public static Set<Address> getMembersLeft(Set<Address> oldSet, Set<Address> newSet) {
      Set<Address> result = new HashSet<Address>(oldSet);
      result.removeAll(newSet);
      return result;
   }
}
