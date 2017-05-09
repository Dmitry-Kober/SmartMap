package org.smartsoftware.smartmap.domain.communication.response;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class ValueResponse implements IResponse {

    private final byte[] value;

    public ValueResponse(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }
}
