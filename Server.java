import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.lang.Exception;
import java.util.Base64;
import java.nio.file.Files;

public class Server {
    private static final String Mserver = "dc37.utdallas.edu";
    private static final int MserverPort = 9037;
    private static String myHostName;
    private static File folder;
    private static int port;    
    private static final int sizeOfFiles = 8 * 1024;// 8 Kilobyte
    private static final int sizeOfheader = 64;

    private static void sendHeartbeat() {
        try (
            Socket socket = new Socket(Mserver, MserverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		) {
            File[] listOfFiles = folder.listFiles();
            StringBuilder outMsg = new StringBuilder("heartbeat:" + Integer.toString(port) + ":");
            for (int i = 0; i < listOfFiles.length; i++) {
                String filename = listOfFiles[i].getName();
                int j = filename.lastIndexOf("/");  // get the file
                outMsg.append(filename.substring(j + 1, filename.length()) + "," + Long.toString(listOfFiles[i].length()) + ":");
            }
            out.println(outMsg.toString());
//            System.out.println(in.readLine()); 
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to Meta Server.");
        }

    }

    public static void listen2Clients() {
		try (
			ServerSocket serverSocket = new ServerSocket(port);
		    ) {	
			while(true)
			{
				Socket clientSocket = serverSocket.accept();
				Thread t = new Thread( new Runnable(){
					public void run() {
                        processFileParts(clientSocket);
						return;
					}
					});
				t.start();
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen.");
        }
    }
    private static void processFileParts(Socket clientSocket) {
        
        try (
            OutputStream outs = clientSocket.getOutputStream();
            InputStream ins = clientSocket.getInputStream()
        ) {
            String[] inputLine;
            byte[] inputHead = new byte[sizeOfheader];
            int headbytes;
            if ((headbytes = ins.read(inputHead)) > 0)  {
                inputLine = new String(Base64.getDecoder().decode(inputHead)).trim().split(":");   //decode the first 64 bytes as the header
                File targetFile = new File(myHostName, inputLine[1]);
                byte[] filePartBody = new byte[sizeOfFiles];
                int bytesAmount = 0;
                switch (inputLine[0]) { 
                    case ("transfer"): {  // store a new filepart
                        Files.copy(ins, targetFile.toPath());
                        break;
                    }
                    case ("read"): {    // read the target filepart
                        Files.copy(targetFile.toPath(), outs);
                        break;
                    }
                    case ("append"): {  
                        if (targetFile.exists()) {
                            // Files.copy(ins, targetFile.toPath(), StandardOpenOption.APPEND);
                            try(
                                FileOutputStream fos = new FileOutputStream(targetFile,true)
                            ) {
                                while ((bytesAmount = ins.read(filePartBody)) > 0) {
                                    // System.out.println(bytesAmount);
                                    fos.write(filePartBody, 0, bytesAmount);
                                }
                                // out.println("Write is done.");
                            } catch (IOException e) {
                                System.err.println("Write failed.");
                            }
                        } else {
                            // Files.copy(ins, targetFile.toPath());
                            try(
                                FileOutputStream fos = new FileOutputStream(targetFile)
                            ) {
                                while ((bytesAmount = ins.read(filePartBody)) > 0) {
                                    // System.out.println(bytesAmount);
                                    fos.write(filePartBody, 0, bytesAmount);
                                }
                            } catch (IOException e) {
                                System.err.println("Write failed.");
                            }
                        }
                        break;
                    }
                    case "create": {
                        Files.createFile(targetFile.toPath());
                        break;
                    }
                    default: {
//                        out.println("Request not recognized.");
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Can not establish communication.");
        }

    }
    public static void main (String args[]) { 
        
        try {
            myHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("Can not get local host name.");
        }
        folder = new File("./" + myHostName + "/");
        port = Integer.parseInt(myHostName.substring(2, 4)) + 9000;

        new Thread(new Runnable() {
            public void run(){
                while (true) {
                    sendHeartbeat();
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted.");
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            public void run() {
                listen2Clients();
            }    
        }).start();
    }
}