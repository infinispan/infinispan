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
package org.infinispan.util;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.infinispan.Version;
import org.infinispan.config.InfinispanConfiguration;

/**
 * Generates XML schema using JAXB annotations from our configuration class hierarchy.
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class JaxbSchemaGenerator {

    public static void main(String[] args) throws Exception {
        try {
            File baseDir = new File(System.getProperty("user.dir"));
            if (args.length > 0) {
                String dir = args[0];
                if (dir != null && dir.length() > 0) {
                    baseDir = new File(dir);
                }
            }

            if (!baseDir.exists())
                baseDir.mkdirs();

            System.out.println("Generating schema file in " + baseDir.getAbsolutePath());
            try {
                JAXBContext context = JAXBContext.newInstance(InfinispanConfiguration.class);
                context.generateSchema(new InfinispanSchemaOutputResolver(baseDir));
                System.out.println("Generated schema file successfully");
            } catch (Exception e) {
                System.err.println("Failed generating schema file " + e);
                e.printStackTrace(System.err);
            }
        } catch (Exception e) {
            System.err.println("Failed generating schema file " + e);
            e.printStackTrace(System.err);
        }
    }

    static class InfinispanSchemaOutputResolver extends SchemaOutputResolver {
        private File dir;

        private InfinispanSchemaOutputResolver(File dir) {
            super();
            this.dir = dir;
        }

        public Result createOutput(String namespaceUri, String suggestedFileName) throws IOException {
            String fname = String.format("infinispan-config-%s.xsd", Version.MAJOR_MINOR);
            System.out.printf("Using file name %s for schema%n", fname);
            return new StreamResult(new File(dir, fname));
        }
    }
}
