package org.smartsoftware.smartmap.domain.communication.request;

import org.smartsoftware.smartmap.domain.data.IKey;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class GetRequest implements IRequest {

    private final IKey key;

    public GetRequest(IKey key) {
        this.key = key;
    }

    public IKey getKey() {
        return key;
    }
}
