package org.infinispan.server.test.query;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoMessage;

import java.io.Serializable;

@ProtoMessage(name = "Transaction")
@ProtoDoc("@Indexed")
public class Transaction implements Serializable {

    @ProtoDoc("@Field")
    @ProtoField(number = 1, name = "size", required = true)
    int size;

    @ProtoDoc("@Field")
    @ProtoField(number = 2, name = "script", required = true)
    String script;


    public Transaction(int size, String script) {
        this.size = size;
        this.script = script;
    }

    public Transaction() {
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "size=" + size +
                ", script='" + script + '\'' +
                '}';
    }
}
