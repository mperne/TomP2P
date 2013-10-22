/*
 * Copyright 2013 Thomas Bocek
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

package net.tomp2p.rpc;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection2.ChannelCreator;
import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.connection2.RequestHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message2;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.p2p.builder.SynchronizationBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.replication.Checksum;
import net.tomp2p.replication.Instruction;
import net.tomp2p.replication.Synchronization;
import net.tomp2p.storage.Data;

/**
 * This Synchronization RPC is used to synchronize data between peers by transferring only changes.
 * 
 * @author Maxat Pernebayev
 * 
 */
public class SynchronizationRPC extends DispatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SynchronizationRPC.class);
    private Synchronization synchronization = null;

    public static final byte INFO_COMMAND = 13;
    public static final byte COPY_COMMAND = 14;
    public static final byte SYNC_COMMAND = 15;

    /**
     * Constructor that registers this RPC with the message handler.
     * 
     * @param peerBean
     *            The peer bean that contains data that is unique for each peer
     * @param connectionBean
     *            The connection bean that is unique per connection (multiple peers can share a single connection)
     */
    public SynchronizationRPC(final PeerBean peerBean, final ConnectionBean connectionBean) {
        super(peerBean, connectionBean, INFO_COMMAND, COPY_COMMAND, SYNC_COMMAND);
        synchronization = new Synchronization();
    }

    /**
     * Sends info message that asks whether the data is present at replica peer or not. If it is present whether it has changed. This is an RPC.
     * 
     * @param remotePeer
     * 				The remote peer to send this request
     * @param synchronizationBuilder
     * 				Used for keeping parameters that are sent
     * @param channelCreator
     * 				The channel creator that creates connections
     * @return	The future response to keep track of future events
     */
    public FutureResponse infoMessage(final PeerAddress remotePeer, final SynchronizationBuilder synchronizationBuilder,
            final ChannelCreator channelCreator) {
        final Message2 message = createMessage(remotePeer, INFO_COMMAND, Type.REQUEST_1);
        
        message.setKey(synchronizationBuilder.getLocationKey());
        message.setKey(synchronizationBuilder.getDomainKey());
        message.setKey(synchronizationBuilder.getContentKey());
        message.setKey(synchronizationBuilder.getHashOfValue());
        message.setLong(System.currentTimeMillis());

        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), synchronizationBuilder);
        LOG.debug("INFO SENT {}" + message);
        System.out.println("INFO SENT: "+message.getSender().getPeerId()+" -> "+message.getRecipient().getPeerId());// + " " +message);
        return requestHandler.sendTCP(channelCreator);
    }
    
    /**
     * Sends copy message that transfers whole data to a replica peer. This is an RPC
     * 
     * @param remotePeer
     * 			The remote peer to send this message
     * @param synchronizationBuilder
     * 			Used for keeping parameters that are sent
     * @param channelCreator
     * 			The channel creator that creates connections
     * @return	The future response to keep track of future events
     */
    public FutureResponse copyMessage(final PeerAddress remotePeer, final SynchronizationBuilder synchronizationBuilder,
            final ChannelCreator channelCreator) {
        final Message2 message = createMessage(remotePeer, COPY_COMMAND, Type.REQUEST_FF_1);
        
        message.setDataMap(synchronizationBuilder.getDataMap());
        message.setLong(System.currentTimeMillis());
        
        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), synchronizationBuilder);
        LOG.debug("COPY SENT {}" + message);
        System.out.println("COPY SENT: "+message.getSender().getPeerId()+" -> "+message.getRecipient().getPeerId());// + " " +message);
        return requestHandler.sendTCP(channelCreator);    	
    }

    /**
     * Sends sync message that transfers the changed parts of data to a replica peer. This is an RPC
     * 
     * @param remotePeer
     * 			The remote peer to send this message
     * @param synchronizationBuilder
     * 			Used for keeping parameters that are sent
     * @param channelCreator
     * 			The channel creator that creates connections
     * @return	The future response to keep track of future events
     * @throws IOException
     */
    public FutureResponse syncMessage(final PeerAddress remotePeer, final SynchronizationBuilder synchronizationBuilder,
            final ChannelCreator channelCreator) throws IOException {
        final Message2 message = createMessage(remotePeer, SYNC_COMMAND, Type.REQUEST_FF_1);

        message.setKey(synchronizationBuilder.getLocationKey());
        message.setKey(synchronizationBuilder.getDomainKey());
        message.setKey(synchronizationBuilder.getContentKey());
        message.setKey(synchronizationBuilder.getHashOfValue());
        message.setLong(System.currentTimeMillis());
        
        ArrayList<Instruction> instructions = synchronizationBuilder.getInstructions();
		Object object = instructions;
		
		message.setBuffer(synchronization.getBuffer(object));

        FutureResponse futureResponse = new FutureResponse(message);
        final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(
                futureResponse, peerBean(), connectionBean(), synchronizationBuilder);
        LOG.debug("SYNC SENT {}" + message);
        System.out.println("SYNC SENT: "+message.getSender().getPeerId()+" -> "+message.getRecipient().getPeerId());// + " " +message);
        return requestHandler.sendTCP(channelCreator);
    }    
    
    @Override
    public Message2 handleResponse(final Message2 message, final boolean sign) throws Exception {
        if (!(message.getCommand() == INFO_COMMAND || message.getCommand() == COPY_COMMAND || message.getCommand() == SYNC_COMMAND)) {
            throw new IllegalArgumentException("Message content is wrong");
        }
        final Message2 responseMessage = createResponseMessage(message, Type.OK);
        switch(message.getCommand()){
        case INFO_COMMAND:
        	return handleInfo(message, responseMessage);
        case COPY_COMMAND: 
        	return handleCopy(message, responseMessage);
        case SYNC_COMMAND:
        	return handleSync(message, responseMessage);
        default:
        	throw new IllegalArgumentException("Message content is wrong");
        }
    }
    
    /**
     * Handles the info message and returns a reply. This is an RPC.
     * 
     * @param message
     * 			The message from a responsible peer
     * @param responseMessage
     * 			The response message to a responsible peer
     * @return	The response message
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws NoSuchAlgorithmException
     */
    private Message2 handleInfo(final Message2 message, final Message2 responseMessage) throws IOException, ClassNotFoundException, NoSuchAlgorithmException {
        System.out.println("INFO RECEIVED: " +message.getSender().getPeerId() + " -> " + message.getRecipient().getPeerId());// + " " +message);
        Number160 locationKey = message.getKey(0);
        Number160 domainKey = message.getKey(1);
        Number160 contentKey = message.getKey(2);
        Number160 hashOfValue = message.getKey(3);
        
        responseMessage.setLong(message.getLong(0));

    	boolean found = false;
    	for (Map.Entry<Number480, Data> entry : peerBean().storage().map().entrySet()) 
    		if(entry.getKey().getLocationKey().equals(locationKey) && entry.getKey().getDomainKey().equals(domainKey) && entry.getKey().getContentKey().equals(contentKey)){
    			found = true;
    			Data data = entry.getValue();
    			
    			if(Number160.createHash(data.object().toString()).equals(hashOfValue))
    				responseMessage.setType(Type.OK);
    			else {
    				ArrayList<Checksum> checksums = synchronization.getChecksums(data.object().toString().getBytes(), Synchronization.SIZE);
    				Object object = checksums;
    				responseMessage.setBuffer(synchronization.getBuffer(object));
    				responseMessage.setType(Type.PARTIALLY_OK);
    			}
    			break;
    		}
    	
    	if(found==false) 
    		responseMessage.setType(Type.NOT_FOUND);

    	return responseMessage;
    }    
    
    /**
     * Handles the copy message by putting whole data into a hash table. This is an RPC.
     * 
     * @param message
     * 			The message from a responsible peer
     * @param responseMessage
     * 			The response message to a responsible peer
     * @return	The response message
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Message2 handleCopy(final Message2 message, final Message2 responseMessage) throws IOException, ClassNotFoundException {
        System.out.println("COPY RECEIVED: " +message.getSender().getPeerId() + " -> " + message.getRecipient().getPeerId());// + " " +message);
        
        for (Map.Entry<Number480, Data> entry : message.getDataMap(0).dataMap().entrySet()) {
        	peerBean().storage().put(entry.getKey().getLocationKey(), entry.getKey().getDomainKey(), entry.getKey().getContentKey(), entry.getValue());
        	
        	if (peerBean().replicationStorage() != null) {
                peerBean().replicationStorage().updateAndNotifyResponsibilities(
                        entry.getKey().getLocationKey());
            }
        }
        
		return responseMessage;
    }
    
    /**
     * Handles the sync message by putting the changed part of data into a hash table. This is an RPC.
     * 
     * @param message
     * 			The message from a responsible peer
     * @param responseMessage
     * 			The response message to a responsible peer
     * @return	The response message
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Message2 handleSync(final Message2 message, final Message2 responseMessage) throws IOException, ClassNotFoundException {
        System.out.println("SYNC RECEIVED: " +message.getSender().getPeerId() + " -> " + message.getRecipient().getPeerId());// + " " +message);
		
		Number160 locationKey = message.getKey(0);
		Number160 domainKey = message.getKey(1);
		Number160 contentKey = message.getKey(2);
		Number160 hashOfValue = message.getKey(3);
        
		// in case calculation takes long time and run() method sends several requests of "SYNC", it prevents all requests after the first one
		String oldValue = "";
		for (Map.Entry<Number480, Data> entry : peerBean().storage().map().entrySet()) 
    		if(entry.getKey().getLocationKey().equals(locationKey)){
    			Data data = entry.getValue();
    			if(Number160.createHash(data.object().toString()).equals(hashOfValue)){
    				System.out.println("ALREADY EXISTS");
    				return message;
    			}
    			else 
    				oldValue = entry.getValue().object().toString();
    		}					

		Object object = synchronization.getObject(message.getBuffer(0));
		ArrayList<Instruction> instructions = (ArrayList<Instruction>) object;
    	
		byte[] reconstructedValue = synchronization.getReconstructedValue(oldValue.getBytes(), instructions, Synchronization.SIZE);
		peerBean().storage().put(locationKey, domainKey, contentKey, new Data(synchronization.byteToString(reconstructedValue)));
		
		if (peerBean().replicationStorage() != null) {
            peerBean().replicationStorage().updateAndNotifyResponsibilities(locationKey);
        }
        
		return responseMessage;
    }    
}
