package org.smartsoftware.smartmap.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.domain.communication.CommunicationChain;
import org.smartsoftware.smartmap.domain.communication.request.*;
import org.smartsoftware.smartmap.utils.FileToFileCollector;
import org.smartsoftware.smartmap.utils.KeyedReentrantLock;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by Dmitry on 23.04.2017.
 */
public class HashBasedRequestManager implements IRequestManager {

    private static final Logger LOG = LoggerFactory.getLogger(HashBasedRequestManager.class);

    private final List<Shard> shards;
    private final KeyedReentrantLock<String> locks = new KeyedReentrantLock<>();

    public HashBasedRequestManager(List<Shard> shards) {
        this.shards = shards;
    }

    @Override
    public CommunicationChain onRequest(CommunicationChain communicationChain) {
        IRequest request = communicationChain.getRequest();

        if (request instanceof PutRequest) {
            String requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Put request for the '{}' key on the '{}' shard.", requestKey, shard.getPath());

            return processPutRequest(communicationChain, requestKey, shard, (PutRequest) request);
        }
        else if (request instanceof GetRequest) {
            String requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Get request for the '{}' key on the '{}' shard.", requestKey, shard.getPath());

            return processGetRequest(communicationChain, requestKey, shard);
        }
        else if (request instanceof RemoveRequest) {
            String requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Remove request for the '{}' key on the '{}' shard.", requestKey, shard.getPath());

            return processRemoveRequest(communicationChain, requestKey, shard);
        }
        else if (request instanceof ListRequest) {
            LOG.trace("Processing a List request.");
            return processListRequest(communicationChain);
        }
        else {
            return processUnknownRequest(communicationChain);
        }
    }

    private Shard identifyShardFor(String requestKey) {
        return shards.get(getHashCodeFrom(requestKey) % shards.size());
    }

    private CommunicationChain processUnknownRequest(CommunicationChain communicationChain) {
        LOG.error("Unknown request is received.");
        return communicationChain.withFailedResponse();
    }

    private CommunicationChain processListRequest(CommunicationChain communicationChain) {
        byte[] registerFile = shards.stream()
                .map(shardItem -> shardItem.getFileSystem().createShardRegister(Paths.get(shardItem.getPath())))
                .collect(new FileToFileCollector());

        return communicationChain.withListResponse(registerFile);
    }

    private CommunicationChain processRemoveRequest(CommunicationChain communicationChain, String requestKey, Shard shard) {
        String fileLocation = shard.getPath() + "/" + requestKey + ".data";

        try {
            locks.writeLock(fileLocation.intern());

            boolean filesIsRemoved = shard.getFileSystem().removeFile(Paths.get(fileLocation));
            if ( !filesIsRemoved ) {
                LOG.error("Unable to remove a file for the '{}' key.", requestKey);
                return communicationChain.withFailedResponse();
            }

            return communicationChain.withSuccessResponse();
        }
        finally {
            locks.writeUnlock(fileLocation);
        }
    }

    private CommunicationChain processGetRequest(CommunicationChain communicationChain, String requestKey, Shard shard) {
        String fileLocation = shard.getPath() + "/" + requestKey + ".data";

        try {
            locks.readLock(fileLocation.intern());

            byte[] value = shard.getFileSystem().getValueFrom(Paths.get(fileLocation));
            if (value.length != 0) {
                return communicationChain.withValueResponse(value);
            }
            else {
                return communicationChain.withEmptyResponse();
            }
        }
        finally {
            locks.readUnlock(fileLocation);
        }
    }

    private CommunicationChain processPutRequest(CommunicationChain communicationChain, String requestKey, Shard shard, PutRequest putRequest) {
        String filePath = shard.getPath() + "/" + requestKey + ".data";

        try {
            locks.writeLock(filePath.intern());

            boolean newFileAdded = shard.getFileSystem().createOrReplaceFileWithValue(Paths.get(filePath), putRequest.getValue());
            if ( !newFileAdded ) {
                LOG.error("Unable to create a new file for the '{}' key.", requestKey);
                return communicationChain.withFailedResponse();
            }

            return communicationChain.withSuccessResponse();
        }
        finally {
            locks.writeUnlock(filePath);
        }
    }

    private int getHashCodeFrom(String key) {
        return key.hashCode();
    }
}
