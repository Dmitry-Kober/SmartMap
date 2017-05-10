package org.smartsoftware.smartmap.domain.communication;

import org.smartsoftware.smartmap.domain.communication.request.IRequest;
import org.smartsoftware.smartmap.domain.communication.response.*;

import java.io.File;
import java.util.List;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class CommunicationChain {

    private static final IResponse EMPTY_RESPONSE = new EmptyResponse();
    private static final IResponse SUCCESS_RESPONSE = new SuccessResponse();
    private static final IResponse FAILED_RESPONSE = new FailedResponse();

    private final IRequest request;
    private IResponse response;

    public CommunicationChain(IRequest request) {
        this.request = request;
    }

    public IRequest getRequest() {
        return request;
    }

    public IResponse getResponse() {
        return response;
    }

    private void setResponse(IResponse response) {
        this.response = response;
    }

    public CommunicationChain withEmptyResponse() {
        checkResponseSetUpAlready();
        this.setResponse(EMPTY_RESPONSE);
        return this;
    }

    public CommunicationChain withSuccessResponse() {
        checkResponseSetUpAlready();
        this.setResponse(SUCCESS_RESPONSE);
        return this;
    }

    public CommunicationChain withValueResponse(byte[] value) {
        checkResponseSetUpAlready();
        this.setResponse(new ValueResponse(value));
        return this;
    }

    public CommunicationChain withFailedResponse() {
        checkResponseSetUpAlready();
        this.setResponse(FAILED_RESPONSE);
        return this;
    }

    public CommunicationChain withListResponse(byte[] register) {
        checkResponseSetUpAlready();
        this.setResponse(new ListResponse(register));
        return this;
    }

    private void checkResponseSetUpAlready() {
        if (this.response != null) {
            throw new IllegalStateException("A response for this communication chain is specified already.");
        }
    }
}
