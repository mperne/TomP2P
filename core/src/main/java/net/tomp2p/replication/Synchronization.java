package net.tomp2p.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.builder.DHTBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.Data;


/**
 * Synchronization class is responsible for efficient and optimal synchronization of data resources between 
 * responsible peer and replica peers. If one of replicas goes offline, the responsible peer transfers the 
 * value completely to the new replica peer. In case the values at responsible peer and replica peer are 
 * the same, then no data is transmitted. If the values are different, then only differences are sent to 
 * the replica peer.
 * 
 * @author Maxat Pernebayev
 *
 */
public class Synchronization {
	public static final int SIZE = 5;
	
	/**
	 * It returns rolling checksum for the offset. The checksum is based on Adler-32 algorithm
	 * 
	 * @param start
	 * 			The start index of offset
	 * @param end
	 * 			The end index of offset
	 * @param offset
	 * 			The offset of the value
	 * @return	The weak checksum
	 */
	public int getAdler(int start, int end, byte[] offset) {
		int len = end-start+1;
		int a=0, b=0;
		for(int i=0; i<len; i++){
			a += (int)offset[start+i];
			b += (len-i)*(int)offset[start+i];
		}
		a = a % 65536;
		b = b % 65536;
		return a+65536*b;
	}	
	
	public int[] getAdlerNew(int start, int end, byte[] offset){
		int len = end-start+1;
		int a=0, b=0;
		for(int i=0; i<len; i++){
			a += (int)offset[start+i];
			b += (len-i)*(int)offset[start+i];
		}
		a = a % 65536;
		b = b % 65536;
		//return a+65536*b;
		int[] result = new int[3];
		result[0] = a;
		result[1] = b;
		result[2] = a+65536*b;
		return result;
	}
	
	/**
	 * It returns MD5 hash for the offset
	 * 
	 * @param offset
	 * 			The offset of the value
	 * @return	The strong checksum
	 * @throws NoSuchAlgorithmException
	 */
	public byte[] getMD5(byte[] offset) throws NoSuchAlgorithmException {
		MessageDigest m=MessageDigest.getInstance("MD5");
		m.update(offset,0,offset.length);
		return new BigInteger(1,m.digest()).toString(16).getBytes();		
	}
	
	/**
	 * @param start
	 * 			The start index of offset
	 * @param end
	 * 			The end index of offset
	 * @param value
	 * 			the value
	 * @return	The offset
	 */
	public byte[] getOffset(int start, int end, byte[] value){
		int size=end-start+1;
		byte[] offset = new byte[size];
		for(int i=0; i<size; i++)
			offset[i]=value[start+i];
		return offset;
	}

	/**
	 * It returns an array of weak and strong checksums for the value.
	 * 
	 * @param value
	 * 			The value
	 * @param size
	 * 			The offset size
	 * @return	The array of checksums
	 * @throws NoSuchAlgorithmException
	 */
	public ArrayList<Checksum> getChecksums(byte[] value, int size) throws NoSuchAlgorithmException {
		ArrayList<Checksum> checksums = new ArrayList<Checksum>();
		int number = value.length/size; 	// number of blocks
		int lsize=0;                        // size of last block
		if(value.length%size!=0) {
			number++;
			lsize = value.length%size; 
		}
		
		for(int i=0; i<number; i++) {
			StringBuilder block = new StringBuilder();
			for(int j=0; j<size; j++){
				if(i==number-1 && j>lsize-1 && lsize!=0) break;
				block.append(value[i*size+j]);
			}
			
			int temp;
			if(i!=number-1)
				temp = size;
			else if(i==number-1 && lsize!=0)
				temp = lsize;
			else temp = size;
				
			
			Checksum checksum = new Checksum();
			checksum.setWeakChecksum(getAdler(i*size, i*size+temp-1, value));
			checksum.setStrongChecksum(getMD5(getOffset(i*size, i*size+temp-1, value)));
			checksums.add(checksum);
		}
		return checksums;
	}	
	
	/**
	 * It checks whether a match is found or not. If it is found returns instruction otherwise null.
	 * 
	 * @param wcs
	 * 			The weak checksum of offset
	 * @param offset
	 * 			The offset
	 * @param checksums
	 * 			The checksums
	 * @return	either instruction or null
	 * @throws NoSuchAlgorithmException
	 */
	public Instruction matches(int wcs, byte[] offset, ArrayList<Checksum> checksums) throws NoSuchAlgorithmException {
		for(int i=0; i<checksums.size(); i++){
			int weakChecksum=checksums.get(i).getWeakChecksum();
			byte[] strongChecksum=checksums.get(i).getStrongChecksum();
			if(weakChecksum==wcs){				
				byte[] md5 = getMD5(offset);
				if(Arrays.equals(strongChecksum,md5)){
					Instruction instruction = new Instruction();
					instruction.setReference(i);
					return instruction;
				}
			}
		}
		return null;
	}	
	
	/**
	 * @param start
	 * 			The start index
	 * @param end
	 * 			The end index
	 * @param newValue
	 * 			The value  at responsible peer
	 * @return	The instruction which contains literal data
	 */
	public Instruction getDiff(int start, int end, byte[] newValue){
		int len = end-start+1;
		byte[] literal = new byte[len];
		for(int i=0; i<len; i++)
			literal[i]=newValue[start+i];
		Instruction instruction = new Instruction();
		instruction.setLiteral(literal);
		return instruction;
	}
	
	public int[] jump(int offset, int blockSize, byte[] newValue) {
		int[] result = new int[2];
		if(offset+blockSize>=newValue.length){
			result[0] = getAdlerNew(offset-1, newValue.length-1, newValue)[0];
			result[1] = getAdlerNew(offset-1, newValue.length-1, newValue)[1];
		}
		else{
			result[0] = getAdlerNew(offset-1, offset+blockSize-2, newValue)[0];
			result[1] = getAdlerNew(offset-1, offset+blockSize-2, newValue)[1];
		}
		return result;
	}
	
	public ArrayList<Instruction> test(byte[] newValue, ArrayList<Checksum> checksums, int blockSize) throws NoSuchAlgorithmException {
		ArrayList<Instruction> result = new ArrayList<Instruction>();
		int a = getAdlerNew(0, blockSize-1, newValue)[0];
		int b = getAdlerNew(0, blockSize-1, newValue)[1];
		int wcs = getAdlerNew(0, blockSize-1, newValue)[2];
		
		int offset = 0;
		int diff = 0;
		Instruction instruction = matches(wcs, getOffset(0, blockSize-1, newValue), checksums);
		if(instruction!=null){
			result.add(instruction);
			offset = blockSize;
			diff = blockSize;
			a = jump(offset, blockSize, newValue)[0];
			b = jump(offset, blockSize, newValue)[1];
//			if(offset+blockSize>=newValue.length){
//				a = getAdlerNew(offset-1, newValue.length-1, newValue)[0];
//				b = getAdlerNew(offset-1, newValue.length-1, newValue)[1];
//			}
//			else{
//				a = getAdlerNew(offset-1, offset+blockSize-2, newValue)[0];
//				b = getAdlerNew(offset-1, offset+blockSize-2, newValue)[1];
//			}
		}
		else{
			offset = 1;
		}
		result = test(result, diff, offset, a, b, newValue, checksums, blockSize);
		return result;
	}
	
	public ArrayList<Instruction> test(ArrayList<Instruction> result, int diff, int offset, int a, int b, byte[] newValue, ArrayList<Checksum> checksums, int blockSize) throws NoSuchAlgorithmException {
		int wcs;
		if(offset+blockSize>=newValue.length) {
			wcs = getAdlerNew(offset, newValue.length-1, newValue)[2];
			Instruction instruction1 = matches(wcs, getOffset(offset, newValue.length-1, newValue), checksums);
			if(instruction1!=null){
				if(diff<offset) 
					result.add(getDiff(diff,offset-1,newValue));
				result.add(instruction1);
			}
			else{
				offset++;
				if(offset>=newValue.length){
					if(diff<offset) {
						result.add(getDiff(diff,newValue.length-1,newValue));
					}
					return result;
				}
				else
					test(result, diff, offset, a, b, newValue, checksums, blockSize);
			}
			return result;
		}

		
		a = (a-newValue[offset-1]+newValue[offset+blockSize-1]) % 65536;
		b = (b-blockSize*newValue[offset-1]+a) % 65536;
		wcs = a + 65536 * b;

		Instruction instruction1 = matches(wcs, getOffset(offset, offset+blockSize-1, newValue), checksums);
		if(instruction1!=null){
			if(diff<offset) 
				result.add(getDiff(diff, offset-1, newValue));
			result.add(instruction1);
			diff = offset + blockSize;
			offset = offset + blockSize;
			a = jump(offset, blockSize, newValue)[0];
			b = jump(offset, blockSize, newValue)[1];
//			if(offset+blockSize>=newValue.length){
//				a = getAdlerNew(offset-1, newValue.length-1, newValue)[0];
//				b = getAdlerNew(offset-1, newValue.length-1, newValue)[1];
//			}
//			else{
//				a = getAdlerNew(offset-1, offset+blockSize-2, newValue)[0];
//				b = getAdlerNew(offset-1, offset+blockSize-2, newValue)[1];
//			}
			test(result, diff, offset, a, b, newValue, checksums, blockSize);
		}
		else{
			offset++;
			test(result, diff, offset, a, b, newValue, checksums, blockSize);
		}
		
		return result;
	}
	
	
	/**
	 * It returns the sequence of instructions each of which contains either reference to a block or literal data.
	 * 
	 * @param newValue
	 * 			The value at responsible peer
	 * @param checksums
	 * 			The array of checksums
	 * @param size
	 * 			The offset size
	 * @return	The sequence of instructions
	 * @throws NoSuchAlgorithmException
	 */	
	public ArrayList<Instruction> getInstructions(byte[] newValue, ArrayList<Checksum> checksums, int size) throws NoSuchAlgorithmException {
		ArrayList<Instruction> result = new ArrayList<Instruction>();
		
		int offsetStart=0;
		int offsetEnd=-1;
		int diffStart=0;
		int diffEnd=-1;
		
		int apre = 0;
		int bpre = 0;		
		for(int i=0; i<size; i++){
			apre += (int)newValue[i];
			bpre += (size-i)*(int)newValue[i];
			offsetEnd = i;
		}
		apre %= 65536;
		bpre %= 65536;
		int wcs = apre+65536*bpre;
		
		int newIndex=0;
		Instruction instruction = matches(wcs, getOffset(offsetStart, offsetEnd, newValue), checksums);
		if(instruction!=null){
			result.add(instruction);
			newIndex = size;
			diffStart = size;
			diffEnd = size-1;
		}
		if(newIndex==0){
			diffEnd++;
			newIndex=1;			
		}
		
		int acur, bcur;
		for(int i=1; i<newValue.length-size+1; i++){
			acur = (apre-newValue[i-1]+newValue[i+size-1]) % 65536;
			bcur = (bpre-size*newValue[i-1]+acur) % 65536;
			wcs = acur + 65536*bcur;
			apre = acur;
			bpre = bcur;
			offsetStart++;			
			offsetEnd++;
			
			if(newIndex>i) continue;

			Instruction instruction1 = matches(wcs, getOffset(offsetStart, offsetEnd, newValue), checksums);
			if(instruction1!=null){
				if(diffStart<=diffEnd) 
					result.add(getDiff(diffStart, diffEnd, newValue));
				result.add(instruction1);
				newIndex = size+i;
				diffStart = offsetEnd+1;
				diffEnd = diffStart-1;
				if(newIndex==newValue.length) 
					offsetStart = offsetEnd+1;
			}			
			if(newIndex==i){
				newIndex=i+1;
				diffEnd = i;
			}
		}
		
		if(offsetStart<=offsetEnd) {
			offsetStart = newIndex;
			
			boolean ok=true;
			while(ok){
				wcs = getAdler(offsetStart, offsetEnd, newValue);
				Instruction instruction1 = matches(wcs, getOffset(offsetStart, offsetEnd, newValue), checksums);
				if(instruction1!=null){
					if(diffStart<=diffEnd) 
						result.add(getDiff(diffStart,diffEnd,newValue));
					diffStart = diffEnd+1;
					result.add(instruction1);
					break;
				}
				if(offsetStart<=offsetEnd){
					diffEnd++; 
					offsetStart++;
				}
				if(offsetStart>offsetEnd) ok=false;
			}
			if(diffStart<=diffEnd) {
				result.add(getDiff(diffStart,diffEnd,newValue));
				diffStart = diffEnd+1;
			}			
		}
		return result;
	}	
	
	/**
	 * It reconstructs the copy of responsible peer's value using instructions and the replica's value.
	 * 
	 * @param oldValue
	 * 			The value at replica
	 * @param instructions
	 * 			The sequence of instructions
	 * @param size
	 * 			The offset size
	 * @return	The value which is identical to the responsible peer's value
	 */
	public byte[] getReconstructedValue(byte[] oldValue, ArrayList<Instruction> instructions, int size) {
		int number = oldValue.length/size;
		int lsize=0;
		if(oldValue.length%size != 0) {
			number++;
			lsize = oldValue.length%size;
		}
		
		int newSize = 0;
		for(int i=0; i<instructions.size(); i++)
			if(instructions.get(i).getReference()==-1)
				newSize += instructions.get(i).getLiteral().length;
			else if(instructions.get(i).getReference()==number-1 && lsize!=0)
				newSize += lsize;
			else newSize += size;
		
		byte[] reconstructedValue = new byte[newSize];
		int count = -1;
		for(int i=0; i<instructions.size(); i++){
			if(instructions.get(i).getReference()==-1){
				byte[] temp = instructions.get(i).getLiteral();
				for(int j=0; j<temp.length; j++){
					count++;
					reconstructedValue[count] = temp[j];
				}
			}
			else if(instructions.get(i).getReference()==number-1 && lsize!=0){
				int reference = instructions.get(i).getReference();
				for(int j=0; j<lsize; j++){
					count++;
					reconstructedValue[count] = oldValue[reference*size+j];
				}
			}
			else {
				int reference = instructions.get(i).getReference();
				for(int j=0; j<size; j++){
					count++;
					reconstructedValue[count] = oldValue[reference*size+j];
				}
			}
		}
		return reconstructedValue;
	}
	
	public String byteToString(byte[] value){
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<value.length; i++)
			sb.append((char)value[i]);
		return sb.toString();
	}
	
	public Buffer getBuffer(Object object) throws IOException {
		Buffer buffer = null;
		ByteArrayOutputStream baos = null;
		ObjectOutput out = null;
		try {
			baos = new ByteArrayOutputStream();
			out = new ObjectOutputStream(baos);   
			out.writeObject(object);
			byte[] bytes = baos.toByteArray();
			ByteBuf buf = Unpooled.wrappedBuffer(bytes);
			buffer = new Buffer(buf);
		} finally {
			out.close();
			baos.close();
		}		
		return buffer;
	}
	
	public Object getObject(Buffer buffer) throws IOException, ClassNotFoundException {
		Object object = null;
		byte[] bytes = new byte[buffer.length()];
		for(int i=0; i<buffer.length(); i++)
			bytes[i] = buffer.buffer().getByte(i);

		ByteArrayInputStream bais = null;
		ObjectInput in = null;
		ArrayList<Checksum> checksums = null;
		try {
			bais = new ByteArrayInputStream(bytes);
			in = new ObjectInputStream(bais);
			object = in.readObject();
		} finally {
			bais.close();
			in.close();
		}		
		return object;
	}

}
