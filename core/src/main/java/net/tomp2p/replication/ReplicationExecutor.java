/*
 * Copyright 2012 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.p2p.builder.SynchronizationStatistics;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implements the default indirect replication.
 * 
 * @author Thomas Bocek
 * 
 */
public class ReplicationExecutor extends TimerTask implements ResponsibilityListener {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationExecutor.class);

    private final StorageGeneric storage;

    private final StorageRPC storageRPC;

    private final Peer peer;

    private final Replication replicationStorage;
    // default replication for put and add is 6
    private static final int REPLICATION = 6;
    
    private final Timer timer;
    private final Random random;
    
    private final int delayMillis;
    
//    private Synchronization synchronization;
    private AutomaticReplication automaticReplication;

    /**
     * Constructor for the default indirect replication.
     * 
     * @param peer
     *            The peer
     */
    public ReplicationExecutor(final Peer peer, final Random random, final Timer timer, final int delayMillis) {
        this.peer = peer;
        this.storage = peer.getPeerBean().storage();
        this.storageRPC = peer.getStoreRPC();
        this.replicationStorage = peer.getPeerBean().replicationStorage();
        replicationStorage.addResponsibilityListener(this);
        replicationStorage.setReplicationFactor(REPLICATION);
        this.random = random;
        this.timer = timer;
        this.delayMillis = delayMillis;
        
//        this.synchronization = new Synchronization();
        this.automaticReplication = new AutomaticReplication(0.999999, peer.getPeerBean().peerMap());
    }
    
    public void init(Peer peer, int intervalMillis) {
        timer.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
    }

    @Override
    public void otherResponsible(final Number160 locationKey, final PeerAddress other, final boolean delayed) {
        LOG.debug("Other peer {} is responsible for {}. I'm {}", other, locationKey, storageRPC.peerBean()
                .serverPeerAddress());
        if(!delayed) {
            final Map<Number480, Data> dataMap = storage.subMap(locationKey);
            sendDirect(other, locationKey, dataMap);
            LOG.debug("transfer from {} to {} for key {}", storageRPC.peerBean().serverPeerAddress(), other,
                    locationKey);
        } else {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    otherResponsible(locationKey, other, false);
                }
            }, random.nextInt(delayMillis));
        }
    }

    @Override
    public void meResponsible(final Number160 locationKey) {
        LOG.debug("I ({}) now responsible for {}", storageRPC.peerBean().serverPeerAddress(), locationKey);
        synchronizeData(locationKey);
    }

    @Override
    public void run() {
        // we get called every x seconds for content we are responsible for. So
        // we need to make sure that there are enough copies. The easy way is to
        // publish it again... The good way is to do a diff
        Collection<Number160> locationKeys = storage.findContentForResponsiblePeerID(peer.getPeerID());

        for (Number160 locationKey : locationKeys) {
            synchronizeData(locationKey);
        }
        
        int replicationFactor = automaticReplication.getReplicationFactor(peer.getPeerID());
		peer.getPeerBean().replicationStorage().setReplicationFactor(replicationFactor);
        
    }

    /**
     * Get the data that I'm responsible for and make sure that there are enough replicas.
     * 
     * @param locationKey
     *            The location key.
     */
    private void synchronizeData(final Number160 locationKey) {
        final Map<Number480, Data> dataMap = storage.subMap(locationKey);
        Number160 domainKeyOld = null;
        Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
            final Number160 domainKey = entry.getKey().getDomainKey();
            final Number160 contentKey = entry.getKey().getContentKey();
            final Data data = entry.getValue();
            LOG.debug("[storage refresh] I ({}) restore {}", storageRPC.peerBean().serverPeerAddress(),
                    locationKey);
            if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                dataMapConverted.put(contentKey, data);
            } else {
                final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                FuturePut futurePut = send(locationKey, domainKey, dataMapConverted1);
                peer.notifyAutomaticFutures(futurePut);
                dataMapConverted.clear();
                dataMapConverted.put(contentKey, data);
            }
            domainKeyOld = domainKey;
        }
        if (!dataMapConverted.isEmpty() && domainKeyOld != null) {
            FuturePut futurePut = send(locationKey, domainKeyOld, dataMapConverted);
            peer.notifyAutomaticFutures(futurePut);
        }
    }

    /**
     * If my peer is responsible, I'll issue a put if absent to make sure all replicas are stored.
     * 
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataMapConverted
     *            The data to store
     * @return The future of the put
     */
    protected FuturePut send(final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConverted) {
		int replicationFactor = replicationStorage.getReplicationFactor();		
		replicationFactor--;
		List<PeerAddress> closePeers = new ArrayList<PeerAddress>();
    	SortedSet<PeerAddress> sortedSet = peer.getPeerBean().peerMap().closePeers(locationKey, replicationFactor);
    	int count=0;
    	for(PeerAddress peerAddress: sortedSet){
    		count++;
    		closePeers.add(peerAddress);
    		extractEachData(peerAddress, locationKey, domainKey, dataMapConverted);
    		if(count==replicationFactor) break;
    	}

    	return null;
    }
    
    /**
     * Extracts each content-value pair from the same location and domain keys
     * 
     * @param other
     * 			The other peer
     * @param locationKey
     * 			The location key
     * @param domainKey
     * 			The domain key
     * @param dataMapConverted
     * 			The data to store
     */
    public void extractEachData(final PeerAddress other, final Number160 locationKey, final Number160 domainKey, final Map<Number160, Data> dataMapConverted){
    	for (Map.Entry<Number160, Data> entry : dataMapConverted.entrySet()) {
    		Number160 contentKey=entry.getKey();
//			Data data = entry.getValue();
//			Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>();
//			dataMapConverted1.put(contentKey, data);
//			try {
//				checkDirect(other, locationKey, domainKey, contentKey, data, dataMapConverted1);				
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
			
			Number480 key = new Number480(locationKey, domainKey, contentKey);
			FutureDone<SynchronizationStatistics> future = peer.synchronize(other).key(key).start();
            future.awaitUninterruptibly();
    	}    	
    }
    
    /**
     * Checks whether the data is present at a replica peer. If it is present whether it is the same or not.
     * 
     * @param other
     * 			The other peer
     * @param locationKey
     * 			The location key
     * @param domainKey
     * 			The domain key
     * @param contentKey
     * 			The content key
     * @param data
     * 			The value
     * @param dataMapConverted
     * 			The data to store
     * @throws ClassNotFoundException
     * @throws IOException
     */
//    public void checkDirect(final PeerAddress other, final Number160 locationKey, final Number160 domainKey, final Number160 contentKey, final Data data, final Map<Number160, Data> dataMapConverted) throws ClassNotFoundException, IOException {	
//    	final String value = data.object().toString();
//    	
//        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 2);
//        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
//            @Override
//            public void operationComplete(final FutureChannelCreator future2) throws Exception {
//                if (future2.isSuccess()) {
//                	SynchronizationBuilder synchronizationBuilder = new SynchronizationBuilder(peer, locationKey, domainKey, contentKey, Number160.createHash(value));
//                	final FutureResponse futureResponse = peer.getSynchronizationRPC().infoMessage(other, synchronizationBuilder, future2.getChannelCreator());
//                	futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
//						@Override
//						public void operationComplete(FutureResponse future) throws Exception {
//							if(future.isFailed()) {
//								Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
//								System.out.println("Failed: "+future.getFailedReason());
//								return;
//							}
//							Message2 responseMessage = future.getResponse();
//							if(responseMessage.getType() == Type.OK){
//								System.out.println("SAME: "+responseMessage.getRecipient().getPeerId()+" <- "+responseMessage.getSender().getPeerId());// + " " +responseMessage);
//								Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
//							}
//							else if(responseMessage.getType() == Type.NOT_FOUND) {
//								System.out.println("NO: "+responseMessage.getRecipient().getPeerId()+" <- "+responseMessage.getSender().getPeerId());// + " " +responseMessage);
//								FutureResponse fr = copy(future2.getChannelCreator(), responseMessage.getSender(), locationKey, domainKey, dataMapConverted);
//								Utils.addReleaseListener(future2.getChannelCreator(), fr, futureResponse);
//							} 
//							else if(responseMessage.getType() == Type.PARTIALLY_OK) {
//								System.out.println("NOT_SAME: "+responseMessage.getRecipient().getPeerId()+" <- "+responseMessage.getSender().getPeerId());// + " " +responseMessage);
//								FutureResponse fr = sync(future2.getChannelCreator(), responseMessage.getSender(), locationKey, domainKey, contentKey, value, responseMessage.getBuffer(0));
//								Utils.addReleaseListener(future2.getChannelCreator(), fr, futureResponse);
//							}
//						}
//                	});
//                    peer.notifyAutomaticFutures(futureResponse);
//                } else {
//                    if (LOG.isErrorEnabled()) {
//                        LOG.error("checkDirect failed " + future2.getFailedReason());
//                    }
//                }
//            }
//        });
//    }    
    
    /**
     * Transferring whole data
     * 
     * @param cc
     * 			The channel creator
     * @param other
     * 			The other peer
     * @param locationKey
     * 			The location key
     * @param domainKey
     * 			The domain key
     * @param dataMapConverted
     * 			The data to store
     * @return	The future response to keep track of future events
     */
//    public FutureResponse copy(final ChannelCreator cc, final PeerAddress other, final Number160 locationKey, final Number160 domainKey, final Map<Number160, Data> dataMapConverted){
//		final DataMap dataMap = new DataMap(locationKey, domainKey, dataMapConverted);
//        SynchronizationBuilder sb = new SynchronizationBuilder(peer, dataMap);
//        FutureResponse futureResponse = peer.getSynchronizationRPC().copyMessage(other, sb, cc);
//        peer.notifyAutomaticFutures(futureResponse);
//        return futureResponse;
//    }
    
    /**
     * Transferring only changed part of data
     * 
     * @param cc
     * 			The channel creator
     * @param other
     * 			The other peer
     * @param locationKey
     * 			The location key
     * @param domainKey
     * 			The domain key
     * @param contentKey
     * 			The content key
     * @param value
     * 			The value
     * @param buffer
     * 			The buffer that contains checksums
     * @return	The future response to keep track of future events 
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
//    public FutureResponse sync(final ChannelCreator cc, final PeerAddress other, final Number160 locationKey, final Number160 domainKey, final Number160 contentKey, final String value, final Buffer buffer) throws ClassNotFoundException, IOException, NoSuchAlgorithmException{
//		Object object = synchronization.getObject(buffer);
//		ArrayList<Checksum> checksums = (ArrayList<Checksum>) object;
//		ArrayList<Instruction> instructions = synchronization.getInstructions(value.getBytes(), checksums, Synchronization.SIZE);
//		SynchronizationBuilder synchronizationBuilder = new SynchronizationBuilder(peer, locationKey, domainKey, contentKey, Number160.createHash(value), instructions);
//		FutureResponse futureResponse = peer.getSynchronizationRPC().syncMessage(other, synchronizationBuilder, cc);
//        peer.notifyAutomaticFutures(futureResponse);
//        return futureResponse;
//    }

    /**
     * If an other peer is responsible, we send this peer our data, so that the other peer can take care of this.
     * 
     * @param other
     *            The other peer
     * @param locationKey
     *            The location key
     * @param domainKey
     *            The domain key
     * @param dataMapConvert
     *            The data to store
     */
    protected void sendDirect(final PeerAddress other, final Number160 locationKey, final Map<Number480, Data> dataMap) {
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    PutBuilder putBuilder = new PutBuilder(peer, locationKey);
                    putBuilder.setDataMap(dataMap);
                    FutureResponse futureResponse = storageRPC.put(other, putBuilder,
                            future.getChannelCreator());
                    Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                    peer.notifyAutomaticFutures(futureResponse);
                } else {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("otherResponsible failed " + future.getFailedReason());
                    }
                }
            }
        });
    }
}
