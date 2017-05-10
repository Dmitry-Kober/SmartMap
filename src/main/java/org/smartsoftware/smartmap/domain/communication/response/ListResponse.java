package org.smartsoftware.smartmap.domain.communication.response;

import java.io.File;

/**
 * Created by dkober on 25.4.2017 г..
 */
public class ListResponse implements IResponse {

    private final byte[] register;

    public ListResponse(byte[] register) {
        this.register = register;
    }

    public byte[] get() {
        return register;
    }
}
