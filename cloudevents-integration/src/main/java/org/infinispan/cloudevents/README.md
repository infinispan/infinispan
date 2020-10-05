= CloudEvents Integration (Experimental)

The Infinispan CloudEvents Integration module converts Infinispan events
to CloudEvents events and sends them to a Kafka topic in structured mode.

This allows Infinispan be further used as a Knative source.

There are two broad kinds of events:

* Cache entry modifications: created, updated, removed, expired
* Audit events: user login, access denied
