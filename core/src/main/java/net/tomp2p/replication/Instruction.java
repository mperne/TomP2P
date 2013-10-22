package net.tomp2p.replication;

import java.io.Serializable;

public class Instruction implements Serializable{
	
	private int reference=-1;
	private byte[] literal=null;
	
	public void setReference(int reference) {
		this.reference = reference;
	}
	
	public void setLiteral(byte[] literal) {
		this.literal = literal;
	}
	
	public int getReference() {
		return reference;
	}
	
	public byte[] getLiteral() {
		return literal;
	}

}
