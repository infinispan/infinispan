Infinispan REST client
======================

This is a simple REST client which currently wraps the JDK HttpClient, although the implementation may be changed at any
time. Its purpose is to be used internally by other Infinispan components for simple interactions with the REST server (
CLI, tests).

It is by no means a complete implementation of an Infinispan client (e.g. it does not implement features like Hot Rod's
topology and hash awareness).
