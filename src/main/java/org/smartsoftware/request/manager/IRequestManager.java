package org.smartsoftware.request.manager;

import org.smartsoftware.domain.communication.CommunicationChain;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IRequestManager {

    void init();

    CommunicationChain onRequest(CommunicationChain communicationChain);

}
