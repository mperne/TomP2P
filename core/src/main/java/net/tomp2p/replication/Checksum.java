package net.tomp2p.replication;

import java.io.Serializable;

public class Checksum implements Serializable{
	private int weakChecksum;
	private byte[] strongChecksum;
	
	public void setWeakChecksum(int weakChecksum) {
		this.weakChecksum = weakChecksum;
	}
	
	public void setStrongChecksum(byte[] strongChecksum) {
		this.strongChecksum = strongChecksum;
	}
	
	public int getWeakChecksum(){
		return weakChecksum;
	}
	
	public byte[] getStrongChecksum(){
		return strongChecksum;
	}
	
}
