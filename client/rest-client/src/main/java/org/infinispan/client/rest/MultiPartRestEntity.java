package org.infinispan.client.rest;

import java.nio.file.Path;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 15.0
 **/
public interface MultiPartRestEntity extends RestEntity {
   MultiPartRestEntity addPart(String name, Path path, MediaType contentType);

   MultiPartRestEntity addPart(String name, String content);
}
