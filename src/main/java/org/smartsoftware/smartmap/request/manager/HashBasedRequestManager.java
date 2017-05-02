package org.smartsoftware.smartmap.request.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartsoftware.smartmap.domain.communication.CommunicationChain;
import org.smartsoftware.smartmap.domain.communication.request.*;
import org.smartsoftware.smartmap.domain.data.IKey;
import org.smartsoftware.smartmap.domain.data.IValue;
import org.smartsoftware.smartmap.utils.KeyedReentrantLock;

import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            IKey requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Put request for the '{}' key on the '{}' shard.", requestKey.get(), shard.getPath());

            return processPutRequest(communicationChain, requestKey, shard, (PutRequest) request);
        }
        else if (request instanceof GetRequest) {
            IKey requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Get request for the '{}' key on the '{}' shard.", requestKey.get(), shard.getPath());

            return processGetRequest(communicationChain, requestKey, shard);
        }
        else if (request instanceof RemoveRequest) {
            IKey requestKey = request.getKey();
            Shard shard = identifyShardFor(requestKey);
            LOG.trace("Processing a Remove request for the '{}' key on the '{}' shard.", requestKey.get(), shard.getPath());

            return processRemoveRequest(communicationChain, requestKey, shard);
        }
        else if (request instanceof ListKeysRequest) {
            LOG.trace("Processing a ListKey request.");
            return processListKeysRequest(communicationChain);
        }
        else {
            return processUnknownRequest(communicationChain);
        }
    }

    private Shard identifyShardFor(IKey requestKey) {
        return shards.get(getHashCodeFrom(requestKey) % shards.size());
    }

    private CommunicationChain processUnknownRequest(CommunicationChain communicationChain) {
        LOG.error("Unknown request is received.");
        return communicationChain.withFailedResponse();
    }

    private CommunicationChain processListKeysRequest(CommunicationChain communicationChain) {
        List<String> allLatestCommittedKeys = new LinkedList<>();
        for (Shard shardItem : shards) {
            Set<String> dataFilesInShard = shardItem.getFileSystem().listAllFilesInShardMatching(Paths.get(shardItem.getPath()), ".*data");
            allLatestCommittedKeys.addAll(
                    dataFilesInShard.stream().map(fileName -> fileName.substring(0, fileName.length()-5)).collect(Collectors.toSet())
            );
        }
        return communicationChain.withListResponse(allLatestCommittedKeys);
    }

    private CommunicationChain processRemoveRequest(CommunicationChain communicationChain, IKey requestKey, Shard shard) {
        String fileLocation = shard.getPath() + "/" + requestKey.get() + ".data";

        try {
            locks.writeLock(fileLocation.intern());

            boolean filesIsRemoved = shard.getFileSystem().removeFile(Paths.get(fileLocation));
            if ( !filesIsRemoved ) {
                LOG.error("Unable to remove a file for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }

            return communicationChain.withSuccessResponse();
        }
        finally {
            locks.writeUnlock(fileLocation);
        }
    }

    private CommunicationChain processGetRequest(CommunicationChain communicationChain, IKey requestKey, Shard shard) {
        String fileLocation = shard.getPath() + "/" + requestKey.get() + ".data";

        try {
            locks.readLock(fileLocation.intern());

            IValue value = shard.getFileSystem().getValueFrom(Paths.get(fileLocation));
            if (value.get().isPresent()) {
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

    private CommunicationChain processPutRequest(CommunicationChain communicationChain, IKey requestKey, Shard shard, PutRequest putRequest) {
        String filePath = shard.getPath() + "/" + requestKey.get() + ".data";

        try {
            locks.writeLock(filePath.intern());


            boolean newFileAdded = shard.getFileSystem().createOrReplaceFileWithValue(Paths.get(filePath), putRequest.getValue());
            if ( !newFileAdded ) {
                LOG.error("Unable to create a new file for the '{}' key.", requestKey.get());
                return communicationChain.withFailedResponse();
            }

            return communicationChain.withSuccessResponse();
        }
        finally {
            locks.writeUnlock(filePath);
        }
    }

    private int getHashCodeFrom(IKey key) {
        return key.get().hashCode();
    }
}
