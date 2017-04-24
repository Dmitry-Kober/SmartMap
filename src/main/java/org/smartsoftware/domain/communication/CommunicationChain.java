package org.smartsoftware.domain.communication;

import org.smartsoftware.domain.communication.request.IRequest;
import org.smartsoftware.domain.communication.response.*;
import org.smartsoftware.domain.data.IValue;

/**
 * Created by dkober on 24.4.2017 г..
 */
public class CommunicationChain {

    private static final IResponse EMPTY_RESPONSE = new EmptyReponse();
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
        this.setResponse(EMPTY_RESPONSE);
        return this;
    }

    public CommunicationChain withSuccessResponse() {
        this.setResponse(SUCCESS_RESPONSE);
        return this;
    }

    public CommunicationChain withValueResponse(IValue value) {
        this.setResponse(new ValueResponse(value));
        return this;
    }

    public CommunicationChain withFailedResponse() {
        this.setResponse(FAILED_RESPONSE);
        return this;
    }
}
