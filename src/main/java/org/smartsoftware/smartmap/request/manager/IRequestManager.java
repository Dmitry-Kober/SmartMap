package org.smartsoftware.smartmap.request.manager;

import org.smartsoftware.smartmap.domain.communication.CommunicationChain;

/**
 * Created by Dmitry on 23.04.2017.
 */
public interface IRequestManager {

    CommunicationChain onRequest(CommunicationChain communicationChain);

}
