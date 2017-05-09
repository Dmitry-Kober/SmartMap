package org.smartsoftware.smartmap;

import org.smartsoftware.smartmap.domain.communication.CommunicationChain;
import org.smartsoftware.smartmap.domain.communication.request.GetRequest;
import org.smartsoftware.smartmap.domain.communication.request.ListKeysRequest;
import org.smartsoftware.smartmap.domain.communication.request.PutRequest;
import org.smartsoftware.smartmap.domain.communication.request.RemoveRequest;
import org.smartsoftware.smartmap.domain.communication.response.IResponse;
import org.smartsoftware.smartmap.domain.communication.response.ListResponse;
import org.smartsoftware.smartmap.domain.communication.response.ValueResponse;
import org.smartsoftware.smartmap.request.manager.HashBasedRequestManager;
import org.smartsoftware.smartmap.request.manager.IRequestManager;
import org.smartsoftware.smartmap.request.manager.Shard;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by dkober on 25.4.2017 г..
 */
public class SmartMap implements ISmartMap {

    private final IRequestManager requestManager;

    public SmartMap() {
        requestManager = new HashBasedRequestManager(
                Collections.singletonList(new Shard("shard1"))
        );
    }

    @Override
    public byte[] get(String key) {
        CommunicationChain communicationChain = requestManager.onRequest(
                new CommunicationChain(new GetRequest(key))
        );
        IResponse response = communicationChain.getResponse();
        if (response instanceof ValueResponse) {
            return ((ValueResponse) response).getValue();
        }
        else {
            return new byte[0];
        }
    }

    @Override
    public void put(String key, byte[] value) {
        requestManager.onRequest(
                new CommunicationChain(new PutRequest(key, value))
        );
    }

    @Override
    public void remove(String key) {
        requestManager.onRequest(
            new CommunicationChain(new RemoveRequest(key))
        );
    }

    @Override
    public Collection<String> listKeys() {
        CommunicationChain communicationChain = requestManager.onRequest(
                new CommunicationChain(new ListKeysRequest())
        );
        IResponse response = communicationChain.getResponse();
        if (response instanceof ListResponse) {
            return ((ListResponse) response).get();
        }
        else {
            return Collections.emptySet();
        }
    }
}
