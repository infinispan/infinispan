package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * Operations for protobuf schema handling.
 *
 * @since 11.0
 **/
public interface RestSchemaClient {

   /**
    * Obtains the names of the registered schemas.
    */
   CompletionStage<RestResponse> names();

   /**
    * Obtains the names of the types.
    */
   CompletionStage<RestResponse> types();

   /**
    * POST a schema with the supplied name and contents.
    */
   CompletionStage<RestResponse> post(String schemaName, String schemaContents);

   /**
    * POST a schema with the supplied name and contents.
    */
   CompletionStage<RestResponse> post(String schemaName, RestEntity schemaContents);

   /**
    * PUT a schema with the supplied name and contents.
    */
   CompletionStage<RestResponse> put(String schemaName, String schemaContents);

   /**
    * PUT a schema with the supplied name and contents.
    */
   CompletionStage<RestResponse> put(String schemaName, RestEntity schemaContents);

   /**
    * DELETE a schema by name.
    */
   CompletionStage<RestResponse> delete(String schemaName);

   /**
    * GET a schema by name.
    */
   CompletionStage<RestResponse> get(String schemaName);
}
