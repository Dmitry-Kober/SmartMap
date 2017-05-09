package org.smartsoftware.smartmap.domain.communication.request;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class PutRequest implements IRequest {

    private final String key;
    private final byte[] value;

    public PutRequest(String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
