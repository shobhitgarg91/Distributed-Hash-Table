import java.io.IOException;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.net.*;
/**
 * Created by Shobhit on 2/9/2017.
 */

/**
 * Class DHTServer implements the Server side in the distributed hash table design. This class is implemented
 * at multiple servers. It listens for a request from the client and performs an action based upon that request.
 */
public class DHTServer {
    static DatagramSocket socket;
    static DatagramSocket recSocket;
    static int sendPort = 7000;
    static int recPort = 8000;
    InetAddress addressReceivedFrom;
    HashMap<String, Integer> receivedFiles = new HashMap<>();
    HashMap<Integer, InetAddress> systemMap;

    /**
     * Constructor function creates the sockets for sending and receiving the UDP packets, it also maps different nodes
     * to their corresponding IPs.
     * @throws IOException
     */
    public DHTServer()throws IOException  {
        socket = new DatagramSocket(sendPort);
        recSocket = new DatagramSocket(recPort);
        systemMap = new HashMap<>();
        systemMap.put(0,InetAddress.getByName("129.21.22.196")); //glados
        systemMap.put(1,InetAddress.getByName("129.21.30.37")); //queeg
        systemMap.put(2,InetAddress.getByName("129.21.34.80")); // comet
        systemMap.put(3,InetAddress.getByName("129.21.37.49")); // rhea
        systemMap.put(4,InetAddress.getByName("129.21.37.42")); // domino
        systemMap.put(5,InetAddress.getByName("129.21.37.55")); // gorgon
        systemMap.put(6,InetAddress.getByName("129.21.37.30")); // kinks

    }
    /**
     * Main function is used to call the function that provides service to the client
     * @param args      Command Line Arguments
     * @throws UnknownHostException
     * @throws SocketException
     */
    public static void main(String args[])  throws IOException {
        DHTServer dhtServer = new DHTServer();
        dhtServer.provideService();
    }

    /**
     * provideService() is used to provide service to the client. This function makes the server always listen to
     * the requests from the client.
     */
    void provideService()   {
        while (true)    {
            receive();
        }
    }

    /**
     * This function is used to receive a request packet from the client and perform actions based upon the request
     * specified. It calls the function to either insert, lookup or copy a file based upon the request. It also provides
     * services to other servers, in that it handles the copy request from other servers.
     */
    void receive()  {
    byte[] receiveBuffer = new byte[2048];
    DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
    try {
        recSocket.receive(receivePacket);
        addressReceivedFrom = receivePacket.getAddress();
    String dataReceived = new String(receivePacket.getData()).trim();
        System.out.println("\nData received: " + dataReceived + "\n");
    String [] data = dataReceived.split(" ");
        // insert a file
        if(data[3].equals("insert"))    {
           insertFile(data[1], data[6], true);
        }
        // look up a file
        else if(data[3].equals("lookup"))   {
            lookupFile(data);

        }
        // copy file
        else if (data[3].equals("copy"))    {
            insertFile(data[1], "", false);

        }

    }
    catch (IOException e){
        System.out.println("\nError in receiving packet\n");
    }

}

    /**
     * This function is used to insert a file in the server. It takes in file name and the client address as
     * parameters. It also provides the functionality of copying a file from another server.
     * @param fileName      name of the file to be copied
     * @param address       address of the client to send acknowledgement
     * @param insert        boolean used to distinguish between insert and copy operations
     * @return              a boolean variable to specify if the operation is successful.
     * @throws UnknownHostException
     */
    boolean insertFile(String fileName, String address, boolean insert) throws UnknownHostException   {
    boolean inserted = false;
    if(!receivedFiles.containsKey(fileName)) {
        receivedFiles.put(fileName, 0);
        System.out.println(fileName + " File Inserted!!\n");
        inserted = true;
        if(insert) {
            String msg = fileName + " successfully inserted at " + InetAddress.getLocalHost();
            address = address.split("/")[1];
            acknowledgeClient(msg, address);
        }
            else
            System.out.println("\nFile copied from upper level: " + fileName + "\n");
    }
    else {
        System.out.println(fileName + " Already present\n");
        String msg = fileName + " already present at " + InetAddress.getLocalHost();
        address = address.split("/")[1];
        acknowledgeClient(msg, address);
        inserted = true;
    }
    return inserted;
}

    /**
     * lookupFile() is used to look up a file in the network. This function checks to see if the file is present in the
     * server. If the file is not present, and if the server is not the root node for the file, it forwards the client
     * request to server at the higher level of hierarchy in the binary tree for that file. It also handles the copying
     * of the file to the lower levels of nodes in case the popularity of the file increases.
     * @param data          File name to be looked up
     * @return              boolean used to specify if the operation was successful
     * @throws UnknownHostException
     */
    boolean lookupFile(String [] data) throws UnknownHostException {
   boolean found = false;
    if(receivedFiles.containsKey(data[1]))  {
        System.out.println("\n" + data[1] + " File present");
        String msg = "File found at " + InetAddress.getLocalHost();
        String address = data[6].split("/")[1];
        acknowledgeClient(msg, address);
        found = true;
        receivedFiles.put(data[1], receivedFiles.get(data[1]) + 1);
        int count = receivedFiles.get(data[1]);
        System.out.println("\n" + data[1] + " file count: " + receivedFiles.get(data[1]));
        // copy to lower branch
        if(count > 1)   {
            int level = Integer.parseInt(data[8]);
            if(level<2)
            copyFileToLowerLevel(data);
            else
                System.out.println("\nFile already present at leaf node");
        }

    }
    // file not present
    else    {

        int level = Integer.parseInt(data[8]);
        int child = Integer.parseInt(data[10]);
        String clientIP = data[6];
        // finding parent child number
        if(level == 1)
            child = 0;
        else if(level == 2) {
            if(child <2)
                child = 0;
            else {
                child = 1;
            }
        }
        if(level == 0)
            System.out.println("\nFile " + data[1] + " not present");
        level --;
            // checking if the root is still to be looked at
            if(level>= 0)   {
            int hashVal = findHash(data[1], level, child);
                InetAddress address = systemMap.get(hashVal);
                StringBuilder sb = new StringBuilder();
                sb.append("name "); sb.append(data[1]);
                sb.append(" operation "); sb.append("lookup");
                sb.append(" client IP "); sb.append(clientIP);
                sb.append(" Level "); sb.append(level);
                sb.append(" Child "); sb.append(child);
                byte[] sendBuffer = sb.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address, recPort);
                try {
                    socket.send(sendPacket);
                }
                catch (IOException e){

                    System.out.println("Issue in sending packet");
                }

            }
            else    {
                // file not present in the system
                String address = data[6].split("/")[1];
                acknowledgeClient("Invalid File Name/ File not present in the system", address);
            }



    }

    return found;
}

    /**
     * copyFileToLowerLevel() is used to send the copy request to the nodes at the lower level.
     * @param data      lookup request that triggered the copy request.
     */
    void copyFileToLowerLevel(String[] data) {
        int level = Integer.parseInt(data[8]);
        int child = Integer.parseInt(data[10]);
        // finding parent child number
        if(level == 1)
            child = 0;
        else if(level == 2) {
            if(child <2)
                child = 0;
            else {
                child = 1;
            }
        }
        level --;
        StringBuilder sb = new StringBuilder();
        sb.append("name "); sb.append(data[1]);
        sb.append(" operation "); sb.append("copy");
        byte[] sendBuffer = sb.toString().getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, addressReceivedFrom, recPort);
        try {
            socket.send(sendPacket);
        }
        catch (IOException e){

            System.out.println("\nIssue in sending packet");
        }
    }

    /**
     * findHash() is used to find the hash value of a file. This function takes in the file name along with the level
     * and the child at the level and gives a unique value as the hash value. Basically, the hash value returned by
     * this function takes into account the level and the child number along with the file name
     * @param name          File name whose hash value is to be found
     * @param level         Level at which the file is to be looked at
     * @param child         Child at the particular level where the file needs to be looked
     * @return              hash value of the file
     */
    int findHash(String name, int level, int child)    {
        if(level == 1 && child == 1)
            child = 5;
        int hash = 0;
        for(char c: name.toCharArray()) {
            if((int)c >= 65 && (int)c<= 90)  {
                hash += (int)c - 64;
            }
            else if((int)c >= 97 && (int)c<= 122)
                hash += (int)c - 96;
        }
        hash += level + child;

        return hash%7;
    }

    /**
     * acknowledgeClient() is used to acknowledge the client after an operation is performed.
     * @param msg       message to be sent to the client
     * @param address   address of the client
     * @throws UnknownHostException
     */
    void acknowledgeClient(String msg, String address) throws UnknownHostException {
        byte[] sendBuffer = msg.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, InetAddress.getByName(address), recPort);
        try {
            socket.send(sendPacket);
        }
        catch (IOException e){

            System.out.println("\nIssue in sending packet");
        }
    }


}
