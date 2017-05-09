package org.smartsoftware.smartmap.domain.communication.request;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class RemoveRequest implements IRequest {

    private final String key;

    public RemoveRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
