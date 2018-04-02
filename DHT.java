import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.net.*;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by Shobhit on 2/9/2017.
 */

/*
Class DHT is used to implement the client side in the distributed hash table design. This class provides an interface
 to the client with the following options:
 1. Inserting a file
 2. Looking up a file
 3. Exiting the program
 It contains a hashmap that maps the nodes with their corresponding IPs. It also contains a custom hash function
 that is used to find the find the nodes on which the file should be present subject to its popularity.
 */

public class DHT {
    static DatagramSocket socket;
    static int sendPort = 7000;
    static int recPort = 8000;
    HashMap<Integer, InetAddress> systemMap;
    DatagramSocket recSocket;

    /**
     * Constructor function creates the sockets for sending and receiving the UDP packets, it also maps different nodes
     * to their corresponding IPs.
     * @throws UnknownHostException
     * @throws SocketException
     */
    public DHT() throws UnknownHostException, SocketException {
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
    public static void main(String args[]) throws UnknownHostException, SocketException {
    DHT dhtObj = new DHT();
    dhtObj.provideService();

    }

    /**
     * provideService() provides the service to the client by giving the option of inserting or looking up a file.
     * It takes the input from the user and depending upon it, performs the required action
     * @throws SocketException
     * @throws UnknownHostException
     */
    void provideService() throws SocketException, UnknownHostException {
        while (true)    {
            Scanner sc = new Scanner(System.in);
            System.out.println("\nPlease select from the following: \n Enter 1 for Inserting a file\n Enter 2 for file lookup \n Enter 3 to Exit");
            int choice = sc.nextInt();
            // insert a file
            if(choice == 1) {
                System.out.println("\nEnter the file Name to insert: ");
                String fileName = sc.next();
                insertFile(fileName);
                receiveAcknowledgement();
            }
            //lookup a file
            else if(choice == 2)    {
                System.out.println("\nEnter the file name to look up: ");
                String  fileName = sc.next();
                lookup(fileName);
                receiveAcknowledgement();
            }
            //exit the program
            else if(choice == 3)    {
                System.out.println("\nEXITING!!");
                System.exit(1);
            }
            // wrong choice
            else
                System.out.println("\nWrong choice, enter again!");
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
     * insertFile() takes in the file name and stores it at the root of the binary tree, that is specific to that
     * file. It stores the file at the root of the binary tree. Thus it finds the hash value of the file at
     * level 0 and child 0.
     * @param fileName      name of the file to be inserted
     * @return              boolean which specifies whether the file has been successfully inserted
     * @throws SocketException
     * @throws UnknownHostException
     */
    boolean insertFile(String fileName) throws SocketException, UnknownHostException {
       boolean fileSent = false;

        StringBuilder sb = new StringBuilder();
        sb.append("name "); sb.append(fileName);
        sb.append(" operation "); sb.append("insert");
        sb.append(" client IP "); sb.append(InetAddress.getLocalHost());
        byte[] sendBuffer = sb.toString().getBytes();
        int hashVal = findHash(fileName, 0, 0);
        InetAddress address = systemMap.get(hashVal);
        System.out.println("\nPacket sent to: " + address.toString());
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address, recPort);
        try {
            socket.send(sendPacket);
            fileSent = true;
        }
        catch (IOException e){

            System.out.println("\nIssue in sending packet");
        }
        return fileSent;
    }

    /**
     * lookup() is used to find the file in the DHT. This function takes in the name of the file as an
     * argument and sends the lookup request to a node at the leaf level of the binary tree for the file.
     * The node at the root level is selected randomly.
     * @param fileName          File name to be looked up
     * @throws UnknownHostException
     */
    void lookup(String fileName) throws UnknownHostException   {
        Random random = new Random();
        int child = random.nextInt(3);
        int hashVal = findHash(fileName, 2,child );
        InetAddress address = systemMap.get(hashVal);
        StringBuilder sb = new StringBuilder();
        sb.append("name "); sb.append(fileName);
        sb.append(" operation "); sb.append("lookup");
        sb.append(" client IP "); sb.append(InetAddress.getLocalHost());
        sb.append(" Level "); sb.append(2);
        sb.append(" Child "); sb.append(child);
        byte[] sendBuffer = sb.toString().getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, address, recPort);
        try {
            socket.send(sendPacket);
        }
        catch (IOException e){

            System.out.println("\nIssue in sending packet");
        }
    }

    /**
     * receiveAcknowledgement() is used by the client to receive acknowledgement from the server after a request
     * has been sent to the server.
     */
    void receiveAcknowledgement() {
        byte[] receiveBuffer = new byte[2048];
        DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
        try {
            recSocket.receive(receivePacket);
            String dataReceived = new String(receivePacket.getData()).trim();
            System.out.println("\n" + dataReceived);

        } catch (IOException e) {
            System.out.println("\nAcknowledgement not received!");
        }
    }

}
