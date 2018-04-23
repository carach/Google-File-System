/*
request format: request:read:filename:offset
                request:append:filename:size
                request:transfer:filename:size
                request:create:filename

heartbeat format:
                heartbeat:port:file1.005,1024:file4.000,4096:file2.100,8192
*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Pattern;
import static java.lang.Math.toIntExact;

public class MServer {
    private static final int portNumber = 9037;
    private static final int sizeOfFiles = 8 * 1024;// 8 Kilobyte per chunk
    private static LinkedList<FileServer> serverList;
    private static HashMap<String, ArrayList<Chunklet>> fileLocation;
    public static class FileServer {
        String hostname;
        int port;
        boolean alive;
        long lastActive;
        public FileServer(String hn, int p) {
            this.hostname = hn;
            this.port = p;
            alive = true;
            lastActive = System.currentTimeMillis();
        }
    }
    public static class Chunklet {
        String name;
        int size;
        String[] location;  // each chunk has multiple replicas
        public Chunklet() {
            this.location = new String[3];
        }
        public Chunklet(String n, int s, String l) {
            this.name = n;
            this.size = s;
            this.location = new String[3];
            this.location[0] = l;  // hostname
        }
    }
    private static final String RNR = "Request not recognized.";
    private static final String NSF = "No such file.";
    private static final String NSC = "No such chunk.";
    private static final String FNA = "File currently unavailable.";
    private static final String SRI = "File server resource inadequate.";

    private static void processMsg(Socket sck) {
        try(
            PrintWriter out = new PrintWriter(sck.getOutputStream(), true);              
            BufferedReader in = new BufferedReader(new InputStreamReader(sck.getInputStream()))
        ) {    
            String hostname = sck.getInetAddress().getHostName();
            long startTime = System.currentTimeMillis();
            String[] inputLine;
            
            if ((inputLine = in.readLine().split(":")) != null) {
                startTime = System.currentTimeMillis();
                if (inputLine[0].equals("heartbeat")) {
                    // System.out.println("Heartbeat received from " + hostname);
                    updateMetadata(hostname, inputLine);
                    out.println("Heartbeat received by M Server.");
                } else if (inputLine[0].equals("request")) {
                    System.out.println("Request received from " + hostname);
                    out.println(specifyLocation(inputLine));
                } else {
                    out.println(RNR);
                }
            } 
//            sck.close();    
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port" + portNumber + " or listening for a connection");
        }
    }

//  check status of all servers
    private static void checkIfOffline() {
        for (FileServer fs: serverList) {
            // System.out.println("lastActive: " + fs.lastActive + " , " + "current time: " + System.currentTimeMillis());    
            if (fs.alive && System.currentTimeMillis() - fs.lastActive > 15 * 1000 ) { // update this server as not alive
                fs.alive = false;
                System.out.println("File server " + fs.hostname + " went offline.");
            }
        }
    }



    //  update metadata upon receiving heartbeat
    private static void updateMetadata(String hostname, String[] message) {
        if (hostname == null || message == null) {
            return;
        }
        if (message.length < 2) {
            return;
        }
        boolean newfs = true;
        for (FileServer fs: serverList) {
            if (fs.hostname.equals(hostname)) {
                fs.port = Integer.parseInt(message[1]);   // when file server already exists in the list, update the port number
                fs.alive = true;
                fs.lastActive = System.currentTimeMillis();
                // System.out.println(fs.hostname + " lastActive updated: " + fs.lastActive);
                newfs = false;
                break;
            }
        }
        if (newfs) {    // add a brand new file server
            System.out.println("File server " + hostname + " started.");
            serverList.add(new FileServer(hostname, Integer.parseInt(message[1])));
        }
        for (int i = 2; i < message.length; i++) {
            String filePartName = message[i].split(Pattern.quote(","))[0];  // split the filepart name and the size
            int size = Integer.parseInt(message[i].split(Pattern.quote(","))[1]);
            int j = filePartName.lastIndexOf(".");
            String filename = filePartName.substring(0,j);
            String PartNo = filePartName.substring(j+1, filePartName.length());  // split the file name and the part number
            if(!fileLocation.containsKey(filename)) {
                fileLocation.put(filename, new ArrayList<Chunklet>());
            }
            int chunkListSize = fileLocation.get(filename).size();
            if (Integer.parseInt(PartNo) >= chunkListSize) {
                for (int k = chunkListSize; k <= Integer.parseInt(PartNo); k++) {   // expand the size of the chunk list when necessary so that new PartNo can be inserted in.
                    fileLocation.get(filename).add(new Chunklet());
                }
                fileLocation.get(filename).set(Integer.parseInt(PartNo), new Chunklet(PartNo, size, hostname));
            } else {
                String[] loc = fileLocation.get(filename).get(Integer.parseInt(PartNo)).location;
                for (String str: loc) {
                    if (str == null) {
                        str = hostname;
                        break;
                    }
                }
            }
        }
    }



    //  specify the location for a requested file
    private static String specifyLocation(String[] message) {
        if (message == null || message.length < 3)
            return RNR;
        String filename = message[2];
        StringBuilder loc = new StringBuilder();
        switch(message[1]) {
            case "read": {
                if (!fileLocation.containsKey(filename)) {
                    return NSF;
                } else {
                    int offset = Integer.parseInt(message[3]);
                    if ( offset == -1) {  // if client specifies -1 which means reading the whole file
                        for (Chunklet chk: fileLocation.get(filename)) {
                            for (FileServer fs: serverList) {
                                if (fs.hostname.equals(chk.location)) {
                                    if (!fs.alive) {   // when the file system is down
                                        return FNA;
                                    } else {
                                        loc.append(chk.location + ":" + fs.port + ",");
                                        break;
                                    }
                                }
                            }
                        }
                        return loc.toString();
                    } else {
                        for (int i = 0; i < fileLocation.get(filename).size(); i++) {
                            Chunklet chk = fileLocation.get(filename).get(i);
                            if ( chk.size >= offset) {
                                for (FileServer fs: serverList) {
                                    if (fs.hostname.equals(chk.location)) {
                                        if (!fs.alive) {   // when the file system is down
                                            return FNA;
                                        } else {
                                            return chk.location + ":" + fs.port + ":" + i + ":" + offset;
                                        }
                                    }
                                }
                            } else {
                                offset = offset - chk.size;
                            }
                        }
                    return NSC; // offset exceeds max file size
                    }
                }
            }
// append response format: dc34.utdallas.edu:9034,8
            case "append": {
                FileServer fs;
                if (!fileLocation.containsKey(filename)) {
                    return NSF;
                } else {
                    int size = Integer.parseInt(message[3]);
                    Chunklet chk = fileLocation.get(filename).get(fileLocation.get(filename).size()-1); // find the last chunklet
                    if (chk.size < sizeOfFiles - size) {   // there is enough space to append it, return just the location of this chunk
                        for (FileServer fse: serverList) {
                            if (fse.hostname.equals(chk.location)) {
                                if (fse.alive) {
                                    return chk.location + ":" +  Integer.toString(fse.port) + "," + Integer.toString(fileLocation.get(filename).size()-1);
                                }
                                else return FNA;
                            }
                        }
                    } else {    // find a new server to store as a new chunklet
                        do {
                            int index = (int)(Math.random() * serverList.size());
                            fs = serverList.get(index);   
                        } while (!fs.alive);    
                        return fs.hostname + ":" + Integer.toString(fs.port) + "," + Integer.toString(fileLocation.get(filename).size());
                    }
                }
            }
            case "transfer": {
                FileServer fs;
                for (int i = 0; i <= Integer.parseInt(message[3]) / sizeOfFiles; i++) {
                    do {
                        int index = (int)(Math.random() * serverList.size());
                        fs = serverList.get(index);   
                    } while (!fs.alive);
                    loc.append(fs.hostname + ":" + Integer.toString(fs.port) + ",");
                }
                return loc.toString();
            }    
            case "create": {
                FileServer fs;
                if (serverList.size() < 3) {
                    return SRI;
                }
                int counter = 0;
                while (counter < 3) {   // requires 3 or more active file servers or loop never ends.
                    do {
                        int index = (int)(Math.random() * serverList.size());
                        fs = serverList.get(index);   
                    } while (!fs.alive);
                    if (loc.indexOf(fs.hostname) == -1) {   // A new server is selected    
                        loc.append(fs.hostname + ":" + Integer.toString(fs.port) + ",");
                        counter++;
                    }
                }
                return loc.toString();
            }
        }
        return RNR;
    }

    public static void main(String[] args) {
        serverList = new LinkedList<>();
        fileLocation = new HashMap<>();
        new Thread (new Runnable() {
            public void run() {
                while (true) {
                    checkIfOffline();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted.");
                    }
                }
            }
        }).start();
        
        try (
            ServerSocket serverSocket = new ServerSocket(portNumber);
        ){   
            while(true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new Runnable() {
                    public void run() {
                        processMsg(clientSocket);
                    }
                }).start();
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen.");
        }
    }	
}