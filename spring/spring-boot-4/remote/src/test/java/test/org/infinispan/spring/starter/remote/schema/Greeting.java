package test.org.infinispan.spring.starter.remote.schema;

import org.infinispan.protostream.annotations.Proto;

@Proto
public record Greeting(String id, String message) {
}
