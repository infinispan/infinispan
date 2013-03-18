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
package org.infinispan.commands;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.ClassFinder;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

@Test(groups = "unit", testName = "commands.CommandIdUniquenessTest")
public class CommandIdUniquenessTest extends AbstractInfinispanTest {
   public void testCommandIdUniqueness() throws Exception {
      List<Class<?>> commands = ClassFinder.isAssignableFrom(ReplicableCommand.class);
      SortedMap<Byte, String> cmdIds = new TreeMap<Byte, String>();

      for (Class<?> c : commands) {
         if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers()) && !LocalCommand.class.isAssignableFrom(c)) {
            log.infof("Testing %s", c.getSimpleName());
            Constructor<?>[] declaredCtors = c.getDeclaredConstructors();
            Constructor<?> constructor = null;
            for (Constructor<?> declaredCtor : declaredCtors) {
               if (declaredCtor.getParameterTypes().length == 0) {
                  constructor = declaredCtor;
                  constructor.setAccessible(true);
                  break;
               }
            }

            ReplicableCommand cmd = (ReplicableCommand) constructor.newInstance();
            byte b = cmd.getCommandId();
            assert b > 0 : "Command " + c.getSimpleName() + " has a command id of " + b + " and does not implement LocalCommand!";
            assert !cmdIds.containsKey(b) : "Command ID [" + b + "] is duplicated in " + c.getSimpleName() + " and " + cmdIds.get(b);
            cmdIds.put(b, c.getSimpleName());
         }
      }

      // TODO Move the command ids to a single file so we can see gaps without actually enforcing it
      // check for gaps.  First ID should be 1.
//      int i = 0;
//      for (Map.Entry<Byte, String> e : cmdIds.entrySet()) {
//         i++;
//         assert e.getKey() == i : "Expected ID " + i + " for command " + e.getValue() + " but was " + e.getKey();
//      }
//
//      System.out.println("Next available ID is " + (i + 1));
   }
}
