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

package net.tomp2p.p2p.builder;

import java.util.ArrayList;

import net.tomp2p.message.DataMap;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.replication.Instruction;

/**
 * 
 * @author Maxat Pernebayev
 * 
 */
public class SynchronizationBuilder extends DHTBuilder<SynchronizationBuilder> {
	
	private Number160 locationKey;
	private Number160 domainKey;
	private Number160 contentKey;
	private Number160 hashOfValue;
	private DataMap dataMap;
	private ArrayList<Instruction> instructions;

    /**
     * Constructor.
     * 
     * @param peer
     * 			The responsible peer that performs synchronization
     * @param locationKey
     * 			The location key
     * @param domainKey
     * 			The domain key
     * @param contentKey
     * 			The content key
     * @param hashOfValue
     * 			The hash of value
     */
    public SynchronizationBuilder(final Peer peer, Number160 locationKey, Number160 domainKey, Number160 contentKey, Number160 hashOfValue) {
        super(peer, peer.getPeerID());
        self(this);
        this.locationKey = locationKey;
        this.domainKey = domainKey;
        this.contentKey = contentKey;
        this.hashOfValue = hashOfValue;
        this.dataMap = null;
        this.instructions = null;
    }
    
    /**
     * Constructor.
     * 
     * @param peer
     * 			The responsible peer that performs synchronization
     * @param dataMap
     * 			The data map
     */
    public SynchronizationBuilder(final Peer peer, final DataMap dataMap) {
        super(peer, peer.getPeerID());
        self(this);
        this.locationKey = null;
        this.domainKey = null;
        this.contentKey = null;
        this.hashOfValue = null;
        this.dataMap = dataMap;
        this.instructions = null;
    }
    
    /**
     * Constructor.
     * 
     * @param peer
     * 			The responsible peer that performs synchronization
     * @param locationKey
     * 			The location key
     * @param domainKey
     * 			The domain key
     * @param contentKey
     * 			The content key
     * @param hashOfValue
     * 			The hash of value
     * @param instructions
     * 			The instructions
     */
    public SynchronizationBuilder(final Peer peer, Number160 locationKey, Number160 domainKey, Number160 contentKey, Number160 hashOfValue, ArrayList<Instruction> instructions) {
        super(peer, peer.getPeerID());
        self(this);
        this.locationKey = locationKey;
        this.domainKey = domainKey;
        this.contentKey = contentKey;
        this.hashOfValue = hashOfValue;
        this.dataMap = null;    	
        this.instructions = instructions;
    }
    
    public Number160 getLocationKey() {
    	return locationKey;
    }
    
    public Number160 getDomainKey() {
    	return domainKey;
    }
    
    public Number160 getContentKey() {
    	return contentKey;
    }
    
    public Number160 getHashOfValue() {
    	return hashOfValue;
    }
    
    public DataMap getDataMap() {
    	return dataMap;
    }
    
    public ArrayList<Instruction> getInstructions() {
    	return instructions;
    }
    
    @Override
    public SynchronizationBuilder setDomainKey(final Number160 domainKey) {
        throw new IllegalArgumentException("Cannot be set here");
    }
}
