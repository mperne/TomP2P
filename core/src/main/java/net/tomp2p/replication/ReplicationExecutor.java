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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.builder.PutBuilder;
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
public class ReplicationExecutor implements ResponsibilityListener, Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationExecutor.class);

    private final StorageGeneric storage;

    private final StorageRPC storageRPC;

    private final Peer peer;

    private final Replication replicationStorage;
    // default replication for put and add is 6
    private static final int REPLICATION = 6;

    /**
     * Constructor for the default indirect replication.
     * 
     * @param peer
     *            The peer
     */
    public ReplicationExecutor(final Peer peer) {
    	System.out.println(peer.getPeerID()+" RE: ReplicationExecutor()");
        this.peer = peer;
        this.storage = peer.getPeerBean().storage();
        this.storageRPC = peer.getStoreRPC();
        this.replicationStorage = peer.getPeerBean().replicationStorage();
        replicationStorage.addResponsibilityListener(this);
        replicationStorage.setReplicationFactor(REPLICATION);
    }

    @Override
    public void otherResponsible(final Number160 locationKey, final PeerAddress other) {
    	System.out.println(peer.getPeerID()+" RE: otherResponsible()");
        LOG.debug("Other peer {} is responsible for {}. I'm {}", other, locationKey, storageRPC.peerBean()
                .serverPeerAddress());

        final Map<Number480, Data> dataMap = storage.subMap(locationKey);
        Number160 domainKeyOld = null;
        Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        for (Map.Entry<Number480, Data> entry : dataMap.entrySet()) {
            final Number160 domainKey = entry.getKey().getDomainKey();
            final Number160 contentKey = entry.getKey().getContentKey();
            final Data data = entry.getValue();
            LOG.debug("transfer from {} to {} for key {}", storageRPC.peerBean().serverPeerAddress(), other,
                    locationKey);

            if (domainKeyOld == null || domainKeyOld.equals(domainKey)) {
                dataMapConverted.put(contentKey, data);
            } else {
                final Map<Number160, Data> dataMapConverted1 = new HashMap<Number160, Data>(dataMapConverted);
                sendDirect(other, locationKey, domainKey, dataMapConverted1);
                dataMapConverted.clear();
            }
            domainKeyOld = domainKey;
        }
        if (!dataMapConverted.isEmpty()) {
            sendDirect(other, locationKey, domainKeyOld, dataMapConverted);
        }
    }

    @Override
    public void meResponsible(final Number160 locationKey) {
    	System.out.println(peer.getPeerID()+" RE: meResponsible()");
        LOG.debug("I ({}) now responsible for {}", storageRPC.peerBean().serverPeerAddress(), locationKey);
        synchronizeData(locationKey);
    }

    @Override
    public void run() {
        // we get called every x seconds for content we are responsible for. So
        // we need to make sure that there are enough copies. The easy way is to
        // publish it again... The good way is to do a diff
    	System.out.println(peer.getPeerID()+" RE: run()");
        Collection<Number160> locationKeys = storage.findContentForResponsiblePeerID(peer.getPeerID());

        for (Number160 locationKey : locationKeys) {
            synchronizeData(locationKey);
        }
    }

    /**
     * Get the data that I'm responsible for and make sure that there are enough replicas.
     * 
     * @param locationKey
     *            The location key.
     */
    private void synchronizeData(final Number160 locationKey) {
    	System.out.println(peer.getPeerID()+" RE: synchronizeData()");
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
    	System.out.println(peer.getPeerID()+" RE: send()");
    	//RequestP2PConfiguration requestP2PConfiguration = new RequestP2PConfiguration(replicationStorage.getReplicationFactor(), 10, 0 );
    	//return peer.put(locationKey).setDataMapContent(dataMapConverted).setDomainKey(domainKey).setRequestP2PConfiguration(requestP2PConfiguration).setPutIfAbsent(true).start();
        return peer.put(locationKey).setDataMapContent(dataMapConverted).setDomainKey(domainKey).setPutIfAbsent(true).start();
    }
    
    protected FuturePut send1(final Number160 locationKey, final Number160 domainKey,
            final Map<Number160, Data> dataMapConverted) {
    	System.out.println(peer.getPeerID()+" RE: send1()");
    	int replicationFactor = replicationStorage.getReplicationFactor();
    	Number160 tContentKey = null;
    	String tValue = "";
    	for (Map.Entry<Number160, Data> entry : dataMapConverted.entrySet()) {
    		tContentKey=entry.getKey();
    		try {
				tValue = entry.getValue().object().toString();
				///tValue = entry.getValue().getData(); //it gives byte array
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	
    	final Number160 contentKey = tContentKey;
    	final String value = tValue;
    	

//    	PeerAddress peerAddress = null;
//    	
//    	SortedSet<PeerAddress> tmp = peer.getPeerBean().getPeerMap().closePeers(locationKey, 5);
//    	System.out.println("Closesest 5 peers: ");
//    	int i=0;
//    	for(PeerAddress pa: tmp){
//    		i++;
//    		System.out.println(pa);
//    		if(i>=5) {
//    			peerAddress = pa;
//    			break;
//    		}
//    	}
//    	System.out.println("peerAddress="+peerAddress);
    	
/*    	//---
    	ArrayList<Object> sendRequest = new ArrayList<Object>();
    	sendRequest.add(System.currentTimeMillis());
    	sendRequest.add("COPY");
    	sendRequest.add(locationKey);
    	sendRequest.add(domainKey);
    	sendRequest.add(contentKey);
    	sendRequest.add(value);
    	
    	RequestP2PConfiguration requestP2PConfiguration = new RequestP2PConfiguration(6, 10, 0 );
    	FutureDHT futureDHT = peer.send(locationKey).setObject(sendRequest).setRequestP2PConfiguration( requestP2PConfiguration ).start();
    	pendingFutures.put(futureDHT, System.currentTimeMillis());
    	
    	try {
			System.out.println("COPY "+System.currentTimeMillis()+" "+(ObjectSize.sizeOf(sendRequest)*6));
		} catch (IOException e) {
			e.printStackTrace();
		}    	
    	//---
*/    	

/*    	
		final Number160 hashOfValue = Number160.createHash(value);
		
    	ArrayList<Object> infoRequest = new ArrayList<Object>();
    	infoRequest.add(System.currentTimeMillis());
    	infoRequest.add("INFO");
    	infoRequest.add(locationKey);
    	infoRequest.add(domainKey);
    	infoRequest.add(contentKey);
    	infoRequest.add(hashOfValue);
    	
    	RequestP2PConfiguration requestP2PConfiguration = new RequestP2PConfiguration(6, 10, 0 );
    	FutureDHT futureDHT = peer.send(locationKey).setObject(infoRequest).setRequestP2PConfiguration( requestP2PConfiguration ).start();
    	pendingFutures.put(futureDHT, System.currentTimeMillis());
    	
    	try {
			System.out.println("INFO "+System.currentTimeMillis()+" "+(ObjectSize.sizeOf(infoRequest)*6));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	futureDHT.addListener(new BaseFutureAdapter<FutureDHT>() {

			@Override
			public void operationComplete(FutureDHT future) throws Exception {
		        for(Object object:future.getRawDirectData2().values()) {
		        	//System.out.println(peer.getPeerID()+" <RESP> "+object);
		        	System.out.println(peer.getPeerID()+" <RESP> ");
		        	ArrayList<Object> response = (ArrayList<Object>)object;
		        	if(response.get(2).equals("NO")){
		        		// send direct
		        		//sendDirect((PeerAddress)response.get(0), locationKey, domainKey, dataMapConverted);

		            	ArrayList<Object> sendRequest = new ArrayList<Object>();
		            	sendRequest.add(System.currentTimeMillis());
		            	sendRequest.add("COPY");		            	
		            	sendRequest.add(locationKey);
		            	sendRequest.add(domainKey);
		            	sendRequest.add(contentKey);
		            	sendRequest.add(value);
		            	
		            	FutureResponse fr = peer.sendDirect((PeerAddress)response.get(0)).setObject(sendRequest).start();
		            	pendingFutures.put(fr, System.currentTimeMillis());		        		

				    	try {
							System.out.println("COPY "+System.currentTimeMillis()+" "+ObjectSize.sizeOf(sendRequest));
						} catch (IOException e) {
							e.printStackTrace();
						}		        	
		        	}
		        	else if(response.get(2).equals("SAME")){
		        		// do nothing
//	        			System.out.println("DO NOTHING "+System.currentTimeMillis()+" 0");
		        	}
		        	else if(response.get(2).equals("NOT_SAME")){
	        			//ArrayList<HashMap<Integer, String>> checksums = (ArrayList<HashMap<Integer, String>>) response.get(3);
	        			ArrayList<Checksum> checksums = (ArrayList<Checksum>) response.get(3);
	        			//System.out.println(checksums.size());
	        			//for(int k=0; k<checksums.size(); k++)
	        			//	System.out.println(checksums.get(k).getWeakChecksum()+":"+checksums.get(k).getStrongChecksum().toString());
						//ArrayList<HashMap<Integer,String>> instructions = synchronization.getInstructions(value, checksums, Synchronization.SIZE);
	        			//ArrayList<HashMap<Integer,String>> instructions = synchronization.getInstructions4(value.getBytes(), checksums, Synchronization.SIZE);
	        			ArrayList<Instruction> instructions = synchronization.getInstructions5(value.getBytes(), checksums, Synchronization.SIZE);
//						System.out.println("instructions("+instructions.size()+")="+instructions);
						
				    	ArrayList<Object> syncRequest = new ArrayList<Object>();
				    	syncRequest.add(response.get(1)); // adding time
				    	syncRequest.add("SYNC");
				    	syncRequest.add(locationKey);
				    	syncRequest.add(domainKey);
				    	syncRequest.add(contentKey);
				    	syncRequest.add(hashOfValue);
				    	syncRequest.add(instructions);
				    	
				    	FutureResponse fr = peer.sendDirect((PeerAddress)response.get(0)).setObject(syncRequest).start(); // peer.sendDirect(peerAddress)
				    	pendingFutures.put(fr, System.currentTimeMillis());
				    	
				    	try {
							System.out.println("SYNC " + System.currentTimeMillis()+" "+ObjectSize.sizeOf(syncRequest));
						} catch (IOException e) {
							e.printStackTrace();
						}				    	
		        	}
		        }    	
			}
		});    	
*/

    	return null;
    	//return peer.put(locationKey).setDataMap(dataMapConverted).setDomainKey(domainKey).setRequestP2PConfiguration(new RequestP2PConfiguration(replicationFactor,replicationFactor,0)).setPutIfAbsent(true).start();
        //return peer.put(locationKey).setDataMap(dataMapConverted).setDomainKey(domainKey).setPutIfAbsent(true).start();
    }    

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
    protected void sendDirect(final PeerAddress other, final Number160 locationKey,
            final Number160 domainKey, final Map<Number160, Data> dataMapConvert) {
    	System.out.println(peer.getPeerID()+" RE: sendDirect()");
        FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future) throws Exception {
                if (future.isSuccess()) {
                    PutBuilder putBuilder = new PutBuilder(peer, locationKey);
                    putBuilder.setDomainKey(domainKey);
                    putBuilder.setDataMapContent(dataMapConvert);
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
