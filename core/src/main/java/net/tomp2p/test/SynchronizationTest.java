package net.tomp2p.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.DataMap;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.p2p.builder.DHTBuilder;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.p2p.builder.SynchronizationBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.replication.Checksum;
import net.tomp2p.replication.Instruction;
import net.tomp2p.replication.Synchronization;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

import org.hamcrest.Matcher;
import org.junit.Test;

public class SynchronizationTest {

	@Test
	public void testGetAdler() throws IOException {
		Synchronization sync = new Synchronization();
		
		String str = "Zurich";
		int a = (int)'Z' + (int)'u' + (int)'r' + (int)'i' + (int)'c' + (int)'h';
		int b = 6*(int)'Z' + 5*(int)'u' + 4*(int)'r' + 3*(int)'i' + 2*(int)'c' + 1*(int)'h';
		
		a %= 65536;
		b %= 65536;
		
		int expectedValue = a + 65536*b;
		
		assertEquals(expectedValue, sync.getAdler(str.getBytes(), 0, 5));
	}

	@Test
	public void testGetMD5() throws IOException, NoSuchAlgorithmException {
		String block = "The quick brown fox jumps over the lazy dog";		
		String expected = "9e107d9d372bb6826bd81d3542a419d6";
		BigInteger bi = new BigInteger(1, Utils.makeMD5Hash(block.getBytes()));
		assertArrayEquals(expected.getBytes(), bi.toString(16).getBytes());
	}	

	@Test
	public void testGetChecksums() throws NoSuchAlgorithmException, IOException {
		Synchronization sync = new Synchronization();
		
		int size=7;
		String value="SwitzerlandZurich";
		
		ArrayList<Checksum> expected = new ArrayList<Checksum>();
		Checksum ch1 = new Checksum();
		Checksum ch2 = new Checksum();
		Checksum ch3 = new Checksum();
		ch1.setWeakChecksum(sync.getAdler("Switzer".getBytes(), 0,6));
		ch1.setStrongChecksum(Utils.makeMD5Hash("Switzer".getBytes()));
		ch2.setWeakChecksum(sync.getAdler("landZur".getBytes(), 0,6));
		ch2.setStrongChecksum(Utils.makeMD5Hash("landZur".getBytes()));
		ch3.setWeakChecksum(sync.getAdler("ich".getBytes(), 0,2));
		ch3.setStrongChecksum(Utils.makeMD5Hash("ich".getBytes()));
		
		expected.add(ch1);
		expected.add(ch2);
		expected.add(ch3);
		
 		assertEquals(expected, sync.getChecksums(value.getBytes(), size));
	}	

	@Test
	public void testMatches() throws IOException, NoSuchAlgorithmException {
		Synchronization sync = new Synchronization();
		
		int size = 6;
		String oldValue = "ZurichGenevaLuganoAAA";

		ArrayList<Checksum> checksums = sync.getChecksums(oldValue.getBytes(), size);		
		byte[] buffer = "Geneva".getBytes();
		int wcs = sync.getAdler(buffer, 0, 5);
		
		Instruction expected = new Instruction();
		expected.setReference(1);

		assertEquals(expected, sync.matches(wcs, buffer, 0, buffer.length, checksums));
	}	

	@Test
	public void testGetDiff() throws IOException, NoSuchAlgorithmException {
		Synchronization sync = new Synchronization();
		
		String newValue = "AzurichGenevaLuganoAbbLuganoAAA";
		String diff = "chGene";
		
		Instruction expected = new Instruction();
		expected.setLiteral(diff.getBytes());

		assertEquals(expected, sync.getDiff(newValue.getBytes(), 5, 10));
	}	
	
	@Test
	public void testGetInstructions() throws IOException, NoSuchAlgorithmException {
		Synchronization sync = new Synchronization();
		
		int size = 6;
		String oldValue = "ZurichGenevaLuganoAAA";
		String newValue = "AzurichGenevaLuganoAbbLuganoAAA";

		ArrayList<Checksum> checksums = sync.getChecksums(oldValue.getBytes(), size);		
 		
 		ArrayList<Instruction> expected = new ArrayList<Instruction>();
 		Instruction ins1 = new Instruction();
 		Instruction ins2 = new Instruction();
 		Instruction ins3 = new Instruction();
 		Instruction ins4 = new Instruction();
 		Instruction ins5 = new Instruction();
 		Instruction ins6 = new Instruction();
 		ins1.setLiteral("Azurich".getBytes());
 		ins2.setReference(1);
 		ins3.setReference(2);
 		ins4.setLiteral("Abb".getBytes());
 		ins5.setReference(2);
 		ins6.setReference(3);
 		
 		expected.add(ins1);
 		expected.add(ins2);
 		expected.add(ins3);
 		expected.add(ins4);
 		expected.add(ins5);
 		expected.add(ins6);
 		
 		assertEquals(expected, sync.getInstructions(newValue.getBytes(), checksums, size)); 		
	}	
	
	@Test
	public void testGetReconstructedValueStatic() throws IOException, NoSuchAlgorithmException {
		// oldValue and newValue are set manually
		Synchronization sync = new Synchronization();
		int size = 6;
		String oldValue = "ZurichGenevaLuganoAAA";
		String newValue = "AzurichGenevaLuganoAbbLuganoAAA";
 		ArrayList<Checksum> checksums = sync.getChecksums(oldValue.getBytes(), size);
 		ArrayList<Instruction> instructions = sync.getInstructions(newValue.getBytes(), checksums, size);
 		
 		byte[] reconstructedValue = sync.getReconstructedValue(oldValue.getBytes(), instructions, size);
 		
 		assertArrayEquals(newValue.getBytes(), reconstructedValue);		
	}	
	
	@Test
    public void testGetReconstructedValueDynamic() throws IOException {
	    for (int i = 0; i < 1000; i++) {
	        testGetReconstructedValueDynamic0();
        }
	}

	private void testGetReconstructedValueDynamic0() throws IOException {
		Synchronization sync = new Synchronization();
		
		int k = 1000;
		int m = 3;  // character types number, m different characters are used to construct content
		//int l = 2;	// number of changes, so l characters of content will be changed
				
		Random random = new Random(2134);
		int l = random.nextInt(k/20)+1;
		int size = random.nextInt(k/15)+1;
		//int size = 3;
//		System.out.print("character types: "+m+" - ");
//		for(int i=0; i<3; i++)
//			System.out.print(" "+(char)(i+65));
//		System.out.println();
//		System.out.println("changes: "+l);
//		System.out.println("content size: "+k);
//		System.out.println("block size: "+size);
		
		String oldValue = "";
		StringBuilder sb = new StringBuilder(k);
		for (int i=0; i<k; i++) {
			int temp = random.nextInt(m);
		    sb.append((char)(temp+65));
		}
		oldValue = sb.toString();
//		System.out.println("oldvalue.length="+oldValue.length());
//		System.out.println("old value: "+oldValue);
		
		String newValue = oldValue;
		for(int i=0; i<l; i++){
			int temp = random.nextInt(k);
			StringBuilder sb1 = new StringBuilder(newValue);
			sb1.setCharAt(temp, 'X');			
			newValue = sb1.toString();
		}
//		System.out.println("new value: "+newValue);
 		
		ArrayList<Checksum> checksums = Synchronization.getChecksums(oldValue.getBytes(), size);
		ArrayList<Instruction> instructions = Synchronization.getInstructions(newValue.getBytes(), checksums, size);
// 		System.out.println("checksums("+checksums.size()+"): "+checksums);
// 		System.out.println("instructions("+instructions.size()+"): "+instructions);
		
 		byte[] reconstructedValue = sync.getReconstructedValue(oldValue.getBytes(), instructions, size);
 		
 		assertArrayEquals(newValue.getBytes(), reconstructedValue);
	}	

    @Test
    public void testInfoMessageSAME() throws IOException, InterruptedException {
        
        final AtomicReference<Type> ref = new AtomicReference<Type>(Type.UNKNOWN_ID);
        final AtomicReference<DataMap> ref2 = new AtomicReference<DataMap>();
        
        final Peer sender = new PeerMaker(new Number160(1)).ports(4001).makeAndListen();
        final Peer receiver = new PeerMaker(new Number160(2)).ports(4002).makeAndListen();
        
        final Number160 locationKey = new Number160(100);
        final Number160 domainKey = DHTBuilder.DEFAULT_DOMAIN;
        final Number160 contentKey = Number160.ZERO;
        final String value = "Test";
        
        HashMap<Number480, Data> map = new HashMap<Number480, Data>();
        final DataMap dataMap = new DataMap(map);
        map.put(new Number480(locationKey, domainKey, contentKey), new Data("Test"));
        
        FutureDHT f1 = sender.put(locationKey).setData(new Data(value)).start();
        f1.awaitUninterruptibly();
        FutureDHT f2 = receiver.put(locationKey).setData(new Data(value)).start();
        f2.awaitUninterruptibly();
        
        sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();
        
        FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (future2.isSuccess()) {
                    SynchronizationBuilder synchronizationBuilder = new SynchronizationBuilder(sender, receiver.getPeerAddress());
                    synchronizationBuilder.dataMap(dataMap);
                    final FutureResponse futureResponse = sender.getSynchronizationRPC().infoMessage(receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
                    futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                        @Override
                        public void operationComplete(FutureResponse future) throws Exception {
                            System.err.println(future.getFailedReason());
                            ref.set(future.getResponse().getType());
                            ref2.set(future.getResponse().getDataMap(0));
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                        }
                    });
                } 
            }
        });
        Thread.sleep(500);
        assertEquals(Type.OK, ref.get());
        assertEquals(1, ref2.get().size());
        assertEquals(0, ref2.get().dataMap().values().iterator().next().toBytes()[0]);
    }

    @Test
    public void testInfoMessageNO() throws IOException, InterruptedException {
        
        final AtomicReference<DataMap> ref = new AtomicReference<DataMap>();
        
        final Peer sender = new PeerMaker(new Number160(3)).ports(4003).makeAndListen();
        final Peer receiver = new PeerMaker(new Number160(4)).ports(4004).makeAndListen();
        
        final Number160 locationKey = new Number160(200);
        final Number160 domainKey = DHTBuilder.DEFAULT_DOMAIN;
        final Number160 contentKey = Number160.ZERO;
        final String value = "Test";
        
        HashMap<Number480, Data> map = new HashMap<Number480, Data>();
        final DataMap dataMap = new DataMap(map);
        map.put(new Number480(locationKey, domainKey, contentKey), new Data("Test"));
        
        FutureDHT f1 = sender.put(locationKey).setData(new Data(value)).start();
        f1.awaitUninterruptibly();
        
        sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();
        
        FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (future2.isSuccess()) {
                    SynchronizationBuilder synchronizationBuilder = new SynchronizationBuilder(sender, receiver.getPeerAddress());
                    synchronizationBuilder.dataMap(dataMap);
                    final FutureResponse futureResponse = sender.getSynchronizationRPC().infoMessage(receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
                    futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                        @Override
                        public void operationComplete(FutureResponse future) throws Exception {
                            ref.set(future.getResponse().getDataMap(0));
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                        }
                    });
                } 
            }
        });
        
        Thread.sleep(500);
        assertEquals(1, ref.get().size());
        assertEquals(1, ref.get().dataMap().values().iterator().next().toBytes()[0]);
    }
    
    private static Type actualNOTSAME;
    
    static void infoMessageNOTSAME(Type a) {
        actualNOTSAME=a;
    }

    @Test
    public void testInfoMessageNOTSAME() throws IOException, InterruptedException {
        
        final AtomicReference<DataMap> ref = new AtomicReference<DataMap>();
        
        final Peer sender = new PeerMaker(new Number160(5)).ports(4005).makeAndListen();
        final Peer receiver = new PeerMaker(new Number160(6)).ports(4006).makeAndListen();
        
        final Number160 locationKey = new Number160(300);
        final Number160 domainKey = DHTBuilder.DEFAULT_DOMAIN;
        final Number160 contentKey = Number160.ZERO;
        final String value = "Test";
        final String value1 = "Test1";
        
        FutureDHT f1 = sender.put(locationKey).setData(new Data(value)).start();
        f1.awaitUninterruptibly();
        FutureDHT f2 = receiver.put(locationKey).setData(new Data(value1)).start();
        f2.awaitUninterruptibly();
        
        HashMap<Number480, Data> map = new HashMap<Number480, Data>();
        final DataMap dataMap = new DataMap(map);
        map.put(new Number480(locationKey, domainKey, contentKey), new Data("Test"));
        
        sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();
        
        FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (future2.isSuccess()) {
                    SynchronizationBuilder synchronizationBuilder = new SynchronizationBuilder(sender, receiver.getPeerAddress());
                    synchronizationBuilder.dataMap(dataMap);
                    final FutureResponse futureResponse = sender.getSynchronizationRPC().infoMessage(receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
                    futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
                        @Override
                        public void operationComplete(FutureResponse future) throws Exception {
                            ref.set(future.getResponse().getDataMap(0));
                            Utils.addReleaseListener(future2.getChannelCreator(), futureResponse);
                        }
                    });
                } 
            }
        });

        Thread.sleep(500);
        assertEquals(1, ref.get().size());
        //assertEquals(1, ref.get().dataMap().values().iterator().next().toBytes()[0]);
    }   

    /*@Test
    public void testCopyMessage() throws IOException, InterruptedException, ClassNotFoundException {
        final Peer sender = new PeerMaker(new Number160(7)).ports(4007).makeAndListen();
        final Peer receiver = new PeerMaker(new Number160(8)).ports(4008).makeAndListen();
        
        final Number160 locationKey = new Number160(400);
        final Number160 domainKey = DHTBuilder.DEFAULT_DOMAIN;
        final Number160 contentKey = Number160.ZERO;
        final String value = "Test";
        final Map<Number160, Data> dataMapConverted = new HashMap<Number160, Data>();
        dataMapConverted.put(contentKey, new Data(value));
        
        FutureDHT f1 = sender.put(locationKey).setData(new Data(value)).start();
        f1.awaitUninterruptibly();
        
        sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();
        
        FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (future2.isSuccess()) {
                    final DataMap dataMap = new DataMap(locationKey, domainKey, dataMapConverted);
                    SynchronizationBuilder sb = new SynchronizationBuilder(sender, dataMap);                    
                    sender.getSynchronizationRPC().copyMessage(receiver.getPeerAddress(), sb, future2.getChannelCreator());
                } 
            }
        });
        
        Thread.sleep(100);
        String secondValue = "";
        for(Map.Entry<Number480, Data> entry: receiver.getPeerBean().storage().map().entrySet())
            if(locationKey.equals(entry.getKey().getLocationKey())){
                Data data = entry.getValue();
                secondValue = data.object().toString();
            }
        assertEquals(value, secondValue);
    }

    @Test
    public void testSyncMessage() throws IOException, InterruptedException, ClassNotFoundException {
        final Peer sender = new PeerMaker(new Number160(9)).ports(4009).makeAndListen();
        final Peer receiver = new PeerMaker(new Number160(10)).ports(4010).makeAndListen();
        final Synchronization synchronization = new Synchronization();
        
        final Number160 locationKey = new Number160(500);
        final Number160 domainKey = DHTBuilder.DEFAULT_DOMAIN;
        final Number160 contentKey = Number160.ZERO;
        final String newValue = "Test1Test2Test3Test4";
        final String oldValue = "test0Test2test0Test4";
        final int size = 5;
        
        FutureDHT f1 = sender.put(locationKey).setData(new Data(newValue)).start();
        f1.awaitUninterruptibly();
        FutureDHT f2 = receiver.put(locationKey).setData(new Data(oldValue)).start();
        f2.awaitUninterruptibly();
        
        sender.bootstrap().setPeerAddress(receiver.getPeerAddress()).start().awaitUninterruptibly();
        
        FutureChannelCreator futureChannelCreator = sender.getConnectionBean().reservation().create(0, 1);
        futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator future2) throws Exception {
                if (future2.isSuccess()) {
                    ArrayList<Checksum> checksums = synchronization.getChecksums(oldValue.getBytes(), size);
                    ArrayList<Instruction> instructions = synchronization.getInstructions(newValue.getBytes(), checksums, size);
                    SynchronizationBuilder synchronizationBuilder = new SynchronizationBuilder(sender, locationKey, domainKey, contentKey, Number160.createHash(newValue), instructions);
                    sender.getSynchronizationRPC().syncMessage(receiver.getPeerAddress(), synchronizationBuilder, future2.getChannelCreator());
                    
                } 
            }
        });
        
        Thread.sleep(100);
        String reconstructedValue = "";
        for(Map.Entry<Number480, Data> entry: receiver.getPeerBean().storage().map().entrySet())
            if(locationKey.equals(entry.getKey().getLocationKey())){
                Data data = entry.getValue();
                reconstructedValue = data.object().toString();
            }
        assertEquals(newValue, reconstructedValue);
    }*/
    
    @Test
    public void testGetObjectAndBuffer() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
        String value = "asdfkjasfalskjfasdfkljasaslkfjasdflaksjdfasdklfjas";
        int size = 10;
        ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
        Buffer buffer = Synchronization.getBuffer(checksums);
        
        assertEquals(checksums, (ArrayList<Checksum>)Synchronization.getObject(buffer));
    }
    
    @Test
    public void testEncodeDecodeChecksums() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
        String value = "asdfkjasfalskjfasdfkljasaslkfjasdflaksjdfasdklfjas";
        int size = 10;
        ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
        byte[] bytes = Synchronization.encodeChecksumList(checksums);
        
        assertEquals(checksums, Synchronization.decodeChecksumList(bytes));
    }
    
    @Test
    public void testEncodeDecodeInstructions() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
        String value = "aabbccddee";
        int size = 5;
        ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
        ArrayList<Instruction> instructions = Synchronization.getInstructions(value.getBytes(), checksums, size);
        byte[] bytes = Synchronization.encodeInstructionList(instructions, new Number160(1234));
        
        assertEquals(instructions, Synchronization.decodeInstructionList(bytes));
    }
    @Test
    public void testEncodeDecodeNumber160() throws NoSuchAlgorithmException, IOException, ClassNotFoundException {
        String value = "aaaaaaaa";
        int size = 2;
        Number160 key = new Number160(12345);
        ArrayList<Checksum> checksums = Synchronization.getChecksums(value.getBytes(), size);
        ArrayList<Instruction> instructions = Synchronization.getInstructions(value.getBytes(), checksums, size);

        System.out.println("--key----------");
        System.out.println(key);
        for(int i=0; i<key.toString().getBytes().length; i++)
        	System.out.print(key.toString().getBytes()[i]+" ");
        System.out.println();
        
        System.out.println("--instructions----");
        //System.out.println(instructions.toString());
        for(int i=0; i<instructions.toString().getBytes().length; i++)
        	System.out.print(instructions.toString().getBytes()[i]+" ");
        System.out.println();
        
        
        byte[] bytes = Synchronization.encodeInstructionList(instructions, key);
        
        System.out.println("--encoded----------");
        System.out.println(bytes+" "+bytes.length);
        for(int i=0; i<bytes.length; i++)
        	System.out.print(bytes[i]);
        System.out.println();
        
        System.out.println("--decoded----------");
        
        Object object = Utils.decodeJavaObject(bytes,0,bytes.length);
        for(int i=0; i<object.toString().getBytes().length; i++)
        	System.out.print(object.toString().getBytes()[i]+" ");
        System.out.println();

//        System.out.println("--decoded----------");
//        
//        Object object1 = Utils.decodeJavaObject(bytes,160,bytes.length);
//        for(int i=0; i<object1.toString().getBytes().length; i++)
//        	System.out.print(object1.toString().getBytes()[i]+" ");
//        System.out.println();

        
        //assertEquals(instructions, Synchronization.decodeInstructionList(bytes));
        assertEquals(key, Synchronization.decodeHash(bytes));
    }    
}
