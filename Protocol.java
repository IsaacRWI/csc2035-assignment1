/*
 * Replace the following string of 0s with your student number
 * 240185299
 */
import java.io.File;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class Protocol {

	static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
	static final String	 TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
	static final String	 LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
	static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
	static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
	public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

	/*
	 * The following attributes control the execution of the transfer protocol and provide access to the 
	 * resources needed for the transfer 
	 * 
	 */ 

	private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
	private int portNumber; 		    // the  port the server is listening on
	private DatagramSocket socket;      // the socket that the client binds to

	private File inputFile;            // the client-side CSV file that has the readings to transfer  
	private String outputFileName ;    // the name of the output file to create on the server to store the readings
	private int maxPatchSize;		   // the patch size - no of readings to be sent in the payload of a single Data segment

	private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server 
	private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

	private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
	private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
	private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

	private int fileTotalReadings;    // number of all readings in the csv file
	private int sentReadings;         // number of readings successfully sent and acknowledged
	private int totalSegments;        // total segments that the client sent to the server

	// Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
	public static Protocol instance = new Protocol();

	/**************************************************************************************************************************************
	 **************************************************************************************************************************************
	 * For this assignment, you have to implement the following methods:
	 *		sendMetadata()
	 *      readandSend()
	 *      receiveAck()
	 *      startTimeoutWithRetransmission()
	 *		receiveWithAckLoss()
	 * Do not change any method signatures, and do not change any other methods or code provided.
	 ***************************************************************************************************************************************
	 **************************************************************************************************************************************/
	/* 
	 * This method sends protocol metadata to the server.
	 * See coursework specification for full details.	
	 */
	public void sendMetadata()   { 
		try{
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));  
			String line;
			while((line = reader.readLine()) != null) {  // if the line in the csv is not null 
				fileTotalReadings++;  // increment fileTotalReadings by 1
			}
			String payloadString = new String(fileTotalReadings+","+outputFileName+","+maxPatchSize);  // creates payload string
			Segment metaSeg = new Segment(0, SegmentType.Meta, payloadString, payloadString.length());  // creates new segment for the metadata

			sendSegment(metaSeg);  // sends metadata segment using the sendSegment method below

			System.out.println("CLIENT:META[SEQ#" + metaSeg.getSeqNum() + "](Number of readings:" + instance.getFileTotalReadings() + ",filename:" + instance.getOutputFileName()  + ",patchSize:" + instance.getMaxPatchSize() + ")");
			
			reader.close();
		}
		catch (IOException e) {
			System.out.println("Error: " + e);
		}
	} 

	// helper method to simplify sending segments
	public void sendSegment(Segment segment) {
		try {
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(outputStream);
			os.writeObject(segment);
			os.flush();
			byte[] data = outputStream.toByteArray();
			DatagramPacket packet = new DatagramPacket(data, data.length, instance.ipAddress, instance.portNumber);
			instance.socket.send(packet);
		} catch (IOException e) {
			System.out.println("Error: " + e);
		}
	}


	public void readAndSend() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			for (int i = 0; i < sentReadings; i++) {  // skips the readings that have already been sent
				reader.readLine();
			}
			String payLoadString = "";  // initializes payload string
			int sqNo = 0;  // initializes sequence number
			if (instance.maxPatchSize > 1) {  // if patch size is greater than 1 
				for (int i = 0; i < maxPatchSize; i++) {  // read patch size number of lines from the csv
					String line = reader.readLine();
					if (line != null) {  // if the read line is not null 
						payLoadString += line + ";";  // add the line to the payload with a semicolon behind it 
					} else {
						break;  // break out of the loop if the line is null  ie end of file reached
					}
				}
				payLoadString = payLoadString.substring(0, payLoadString.length() - 1);  // remove the last semicolon from payload string
				// sentReadings += maxPatchSize;    // incrementing sentReadings moved to receiveAck method and only increments when ack for segment is received instead of sent
				sqNo = ((sentReadings / maxPatchSize) + 1) % 2;  // cycles sequence number between 0 and 1 starting on 1 
			} else {  // if patch size is 1 				
				payLoadString = reader.readLine();  // reads a single line from the csv and sets it as the payload 
				// sentReadings += 1;
				sqNo = (sentReadings + 1) % 2; // same thing cycles sequence number between 0 and 1 starting on 1
			}
				dataSeg = new Segment(sqNo, SegmentType.Data, payLoadString, payLoadString.length());  // creates the data segment with the payload and sequence number
				sendSegment(dataSeg);  // sends data segment with helper method
				totalSegments++;  // increment total segments sent
				System.out.println("CLIENT:Send:DATA[SEQ#" + sqNo + "](size:" + payLoadString.length() + ",crc:" + dataSeg.calculateChecksum() +",content:" + payLoadString +")");
				payLoadString = "";  // empty payload string for next payload
				reader.close();
		} catch (IOException e) {
			System.out.println("Error: " + e);
		}
	}
	

	/* 
	 * This method receives the current Ack segment (ackSeg) from the server 
	 * See coursework specification for full details.
	 */
	public boolean receiveAck() throws SocketTimeoutException { 
		Segment ackSeg = new Segment(); 
		byte[] buf = new byte[Protocol.MAX_Segment_SIZE]; //prepare the buffer to have the max segment size
		try{
			DatagramPacket incomingPacket = new DatagramPacket(buf, buf.length);
			instance.socket.receive(incomingPacket);
			byte[] data = incomingPacket.getData();
			ByteArrayInputStream in = new ByteArrayInputStream(data);
			ObjectInputStream is = new ObjectInputStream(in);

			ackSeg = (Segment) is.readObject(); 

			if (ackSeg.getType() == SegmentType.Ack) {
				if (ackSeg.getSeqNum() == instance.dataSeg.getSeqNum()) {
					System.out.println("CLIENT:RECEIVE:ACK[SEQ#" + ackSeg.getSeqNum() + "]");
					System.out.println("**********************************************************************");
					// instance.sentReadings += instance.dataSeg.getPayLoad().split(";").length;
					int n = instance.dataSeg.getPayLoad() == null || instance.dataSeg.getPayLoad().isEmpty() ? 0 : instance.dataSeg.getPayLoad().split(";").length;
					instance.sentReadings += n;
					// System.out.println("sentReadings: " + instance.sentReadings);
					instance.ackSeg = ackSeg;
					if (instance.sentReadings >= instance.fileTotalReadings) {
						System.out.println("Total segments:" + instance.totalSegments);
						System.exit(0);
						return true;
					} else {
						return true;
					}
				} else {
					System.out.println("CLIENT:RECEIVE:ACK[SEQ#" + ackSeg.getSeqNum() + "] OUT OF ORDER");
					return false;
				}
			}
		} catch (SocketTimeoutException ste)  {
			throw ste;
		} catch (ClassNotFoundException e) {
			System.out.println("Error: " + e);
		} catch (IOException e) {
			System.out.println("Error: " + e);
		}
		// System.exit(0);
		return false;
	}

	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission() {
		try{
			instance.socket.setSoTimeout(instance.timeout);
		}catch (SocketException e) {
			System.out.println("Error: " + e);
		}
		while (true) {
			try {
				boolean matched = receiveAck();
				if (matched) {
					instance.currRetry = 0;
					break;
				}
			} catch (SocketTimeoutException ste) {
				instance.currRetry += 1;
				if (instance.currRetry > instance.maxRetries) {
					System.out.println("ERROR: maximum retransmissions (" + instance.maxRetries +
									") reached for seq#" + instance.dataSeg.getSeqNum() + ". Terminating.");
					System.exit(1);
				}
				System.out.println("CLIENT:TIMEOUT for SEQ#" + instance.dataSeg.getSeqNum() + ", Current Retry: " + instance.currRetry + "/" + instance.maxRetries + "");
				System.out.println("CLIENT:Send:DATA[SEQ#" + instance.dataSeg.getSeqNum() + "](size:" + instance.dataSeg.getPayLoad().length() + ",crc:" + instance.dataSeg.calculateChecksum() +",content:" + instance.dataSeg.getPayLoad() +")");
				sendSegment(instance.dataSeg);
				instance.totalSegments += 1;
			}
		}
	}

	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss) throws IOException{
		byte[] buf = new byte[Protocol.MAX_Segment_SIZE];
		
		//creat a temporary list to store the readings
		List<String> receivedLines = new ArrayList<>();

		//track the number of the correctly received readings
		int readingCount= 0;

		int usefulBytes = 0;
		int totalBytes = 0;

		int sqNo = 0;

		try{
			serverSocket.setSoTimeout(2000);
		}catch (SocketException e) {
			System.out.println("Error: " + e);
		}

		// while still receiving Data segments  
		while (true) {
			try{
				DatagramPacket incomingPacket = new DatagramPacket(buf, buf.length);
				serverSocket.receive(incomingPacket);// receive from the client  

				sqNo = ((readingCount / maxPatchSize) + 1) % 2;
				System.out.println("Expected SEQ#:" + sqNo);
			
				Segment serverDataSeg = new Segment(); 
				byte[] data = incomingPacket.getData();
				ByteArrayInputStream in = new ByteArrayInputStream(data);
				ObjectInputStream is = new ObjectInputStream(in);

				// read and then print the content of the segment
				try {
					serverDataSeg = (Segment) is.readObject(); 
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}


				System.out.println("SERVER: Receive: DATA [SEQ#"+ serverDataSeg.getSeqNum()+ "]("+"size:"+serverDataSeg.getSize()+", crc: "+serverDataSeg.getChecksum()+
						", content:"  + serverDataSeg.getPayLoad()+")");

				// calculate the checksum
				long x = serverDataSeg.calculateChecksum();

				// if the calculated checksum is same as that of received checksum then send the corresponding ack
				if (serverDataSeg.getType() == SegmentType.Data && x == serverDataSeg.getChecksum()) {
					System.out.println("SERVER: Calculated checksum is " + x + "  VALID");
					InetAddress iPAddress = incomingPacket.getAddress();
					int port = incomingPacket.getPort();

					String[] lines = serverDataSeg.getPayLoad().split(";");

					if (serverDataSeg.getSeqNum() != sqNo) {
						System.out.println("SERVER: Duplicate DATA Detected");
						System.out.println("****************************************************"); 
						totalBytes += serverDataSeg.getSize();
					} else {
						readingCount += lines.length;
						usefulBytes += serverDataSeg.getSize();
						totalBytes += serverDataSeg.getSize();
						// System.out.println("writing payload to list.......................................................");
						receivedLines.add("Segment ["+ serverDataSeg.getSeqNum() + "] has "+ lines.length + " Readings");
						receivedLines.addAll(Arrays.asList(lines));
						receivedLines.add("");
					}
					

					if (!isLost(loss)) {
						Server.sendAck(serverSocket, iPAddress, port, serverDataSeg.getSeqNum());
					} else {
						System.out.println("SERVER: Simulating ACK loss. ACK[SEQ#" + serverDataSeg.getSeqNum() + "] is lost.");
						System.out.println("===============================================");
						System.out.println("===============================================");
					}
					
					
				
				// if the calculated checksum is not the same as that of received checksum, then do not send any ack
				} else if (serverDataSeg.getType() == SegmentType.Data&& x != serverDataSeg.getChecksum()) {
					System.out.println("SERVER: Calculated checksum is " + x + "  INVALID");
					System.out.println("SERVER: Not sending any ACK ");
					System.out.println("*************************** "); 
				}
				
				//if all readings are received, then write the readings to the file
				if (Protocol.instance.getOutputFileName() != null && readingCount >= Protocol.instance.getFileTotalReadings()) {
					// System.out.println("All readings received. Writing to file..."); 
					Server.writeReadingsToFile(receivedLines, Protocol.instance.getOutputFileName());
					break;
				}
			} catch (SocketTimeoutException ste) {
				System.out.println("SERVER: Timout reached. EXITING");
				break;
			}
		}
		System.out.println("Total useful bytes received: " + usefulBytes);
		System.out.println("Total bytes received: " + totalBytes);
		System.out.println("Efficiency: " + (usefulBytes * 100 / totalBytes) + "%");
		serverSocket.close();
	}


	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}
