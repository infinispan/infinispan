/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.marshall.jboss;

import org.infinispan.CacheException;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.GlobalTransactionFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Test the behaivour of JBoss Marshalling based {@link Marshaller} implementation 
 * which is {@link JBossMarshaller}}. This class should contain methods that exercise
 * logic in this particular implementation.
 */
public class JBossMarshallerTest extends AbstractInfinispanTest {

   private final VersionAwareMarshaller marshaller = new VersionAwareMarshaller();

   private GlobalTransactionFactory gtf = new GlobalTransactionFactory();

   @BeforeTest
   public void setUp() {
      marshaller.inject(Thread.currentThread().getContextClassLoader(), new RemoteCommandsFactory());
      marshaller.start();
   }

   @AfterTest
   public void tearDown() {
      marshaller.stop();
   }
   
   public void testDuplicateExternalizerId() throws Exception {
      JBossMarshaller jbmarshaller = new JBossMarshaller();
      ConstantObjectTable.MARSHALLABLES.add(DuplicateIdClass.class.getName());
      try {
         jbmarshaller.start(Thread.currentThread().getContextClassLoader(), new RemoteCommandsFactory(), marshaller);
         assert false : "Should have thrown a CacheException reporting the duplicate id";
      } catch (CacheException ce) {
      } finally {
         jbmarshaller.stop();
         ConstantObjectTable.MARSHALLABLES.remove(DuplicateIdClass.class.getName());
      }
   }
   
   @Marshallable(externalizer = DuplicateIdClass.Externalizer.class, id = Ids.ARRAY_LIST)
   static class DuplicateIdClass {
      public static class Externalizer implements org.infinispan.marshall.Externalizer {
         public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return null;
         }

         public void writeObject(ObjectOutput output, Object object) throws IOException {
         }
      }
   }

}
