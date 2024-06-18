String cacheName = "CacheWithXMLConfiguration";
String xml = String.format("<distributed-cache name=\"%s\" mode=\"SYNC\">" +
                              "<encoding media-type=\"application/x-protostream\"/>" +
                              "<locking isolation=\"READ_COMMITTED\"/>" +
                              "<transaction mode=\"NON_XA\"/>" +
                              "<expiration lifespan=\"60s\" interval=\"20s\"/>" +
                           "</distributed-cache>" , cacheName);
remoteCacheManager.administration().getOrCreateCache(cacheName, new XMLStringConfiguration(xml));
