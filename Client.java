import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Base64;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Arrays;

public class Client {
    private static final String Mserver = "dc37.utdallas.edu";
    private static final int MserverPort = 9037;
    private static final int sizeOfFiles = 8 * 1024;// 8 Kilobyte
    private static final int sizeOfheader = 48;
    private static String myHostName;

    private static final String RNR = "Request not recognized.";
    private static final String NSF = "No such file.";
    private static final String NSC = "No such chunk.";
    private static final String FNA = "File currently unavailable.";

    public static void createFile(String filename) {
        String dest = "";
        try (
            Socket socket = new Socket(Mserver, MserverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
            new InputStreamReader(socket.getInputStream()))
        ) {
            out.println("request:create" + ":" + filename);
            dest = in.readLine();
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to Meta Server.");
        }
        if (!dest.isEmpty()) {
            try (
                Socket socket = new Socket(dest.split(":")[0], Integer.parseInt(dest.split(":")[1]) );
                OutputStream outs= socket.getOutputStream();
                InputStream ins = socket.getInputStream()
            ) {
                String outHeader = "create" + ":" + filename + ".000";
                byte[] header = new byte[sizeOfheader];
                header = Base64.getEncoder().encode((String.format("%-" + Integer.toString(sizeOfheader) + "s", outHeader)).getBytes());   // pad the outgoing message to 64 bytes and encode it to binary format
                outs.write(header);
            } catch (IOException e) {
                System.err.println("Couldn't get I/O for the connection to the file server.");
            }
            System.out.println("New file created.");
        } else {
            System.out.println("File creation failed.");
        }
    }

    public static void transferFile(File f) {

        int partCounter = 0;    // name parts from 0, 1, 2, 3, ...
        ArrayList<String> dest = new ArrayList<>();
        byte[] filePartBody = new byte[sizeOfFiles];
        // send request to metadata server
        try (
            Socket socket = new Socket(Mserver, MserverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
            new InputStreamReader(socket.getInputStream()))
		) {
            out.println("request:transfer" + ":" + f.getName() + ":" + Long.toString(f.length()));
            String[] parse = in.readLine().split(",");
//            System.out.println(parse.length);
            for(int i = 0; i < parse.length; i++) {
//                System.out.println(parse[i]);
                dest.add(parse[i]);
            }
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to Meta Server.");
        }
        // send file body to file servers
        if (dest.size() > 0) {
            try (
                FileInputStream fis = new FileInputStream(f);
//                BufferedInputStream bis = new BufferedInputStream(fis)
                ) {
                int bytesAmount = 0;
                for (int i = 0; i < dest.size(); i++) {
                    //write each chunk of data into separate file with different number in name
                    String filePartName = String.format("%s.%03d", f.getName(), partCounter++);
                    File newFile = new File(filePartName);
//                    StringBuilder outMsg = new StringBuilder(filePartName + ":");

                    try (
                        Socket socket = new Socket(dest.get(i).split(":")[0], Integer.parseInt(dest.get(i).split(":")[1]) );
                        OutputStream outs = socket.getOutputStream();
                        ) {
                        String outHeader = "transfer" + ":" + filePartName;
                        byte[] header = new byte[sizeOfheader];
                        header = Base64.getEncoder().encode((String.format("%-" + Integer.toString(sizeOfheader) + "s", outHeader)).getBytes());   // pad the outgoing message to 64 bytes and encode it to binary format
                        if ((bytesAmount = fis.read(filePartBody)) > 0) {                       
//                            System.out.println(bytesAmount);
                            // combine header and body
                            byte[] combined = new byte[header.length + bytesAmount];
                            System.arraycopy(header,0,combined,0         ,header.length);
                            System.arraycopy(filePartBody,0,combined,header.length,bytesAmount);
                            outs.write(combined, 0, header.length + bytesAmount);
                            outs.flush();
                        } 
                    } catch (IOException e) {
                        System.err.println("Couldn't get I/O for the connection to the file server.");
                    }
                    
                }
            } catch (IOException e) {
                System.err.println("Open file failed.");
            }
            System.out.println("File transfered.");
        }
    }

    public static void readFile(String filename, int offset) {
        ArrayList<String> dest = new ArrayList<>();
        File targetfile = new File(myHostName, filename);
        try (
            Socket socket = new Socket(Mserver, MserverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
		) {
            
            out.println("request:read:" + filename + ":" + offset);
            String[] parse = in.readLine().split(",");
            //  if there is one or more file parts are unavalibale
            if (parse[0].equals(FNA)) {
                System.out.println(FNA);
                return;
            }
            //  if there is no such a file on server
            if (parse[0].equals(NSF)) {
                System.out.println(NSF);
                return;
            }
            //  if there is no such a chunk of the file
            if (parse[0].equals(NSC)) {
                System.out.println(NSC);
                return;
            }
            //System.out.println(parse.length);
            for(int i = 0; i < parse.length; i++) {
                dest.add(parse[i]);
            }
            
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to file server.");
        }
        
        if (offset == -1) {  // read all chunks and connect them as a whole file
            ArrayList<File> partList = new ArrayList<>();        
            byte[] filePartBody = new byte[sizeOfFiles];
            int bytesAmount = 0;
            for (int i = 0; i < dest.size(); i++) {
                File part = new File(myHostName, String.format("%s.%03d", filename, i));
                try (
                    Socket socket = new Socket(dest.get(i).split(":")[0], Integer.parseInt(dest.get(i).split(":")[1]));
                    OutputStream outs = socket.getOutputStream();
                    InputStream ins = socket.getInputStream()  
                ) {
                    String outHeader = "read" + ":" + part.getName();
                    byte[] header = new byte[sizeOfheader];
                    header = Base64.getEncoder().encode((String.format("%-" + Integer.toString(sizeOfheader) + "s", outHeader)).getBytes());   // pad the outgoing message to 64 bytes and encode it to binary format
                    outs.write(header);
                    Files.copy​(ins, part.toPath());
                    // try (
                    //     FileOutputStream fos = new FileOutputStream(part)
                    // ) {
                    //     while ((bytesAmount = ins.read(filePartBody)) > 0) {
                    //         System.out.println(bytesAmount);
                    //         fos.write(filePartBody, 0, bytesAmount);
                    //     }
                    // } catch (IOException e) {
                    //     System.err.println("Write is not done.");
                    // }

                } catch (IOException e) {
                    System.err.print("");
                }
                partList.add(part);
            }   
            
            try (
                FileOutputStream fos = new FileOutputStream(targetfile)
            ) {
                for (File f : partList) {
                    Files.copy(f.toPath(), fos);
                    Files.delete(f.toPath());
                }
                System.out.println("File is ready to be read at " + targetfile.getAbsolutePath());
            } catch (IOException e) {
                System.err.println("Get file failed, please try again.");
            }
        } else {
            int partNo = Integer.parseInt(dest.get(0).split(":")[2]);
            int startPoint = Integer.parseInt(dest.get(0).split(":")[3]);
            File part = new File(myHostName, String.format("%s.%03d", filename, partNo));
            char[] filePartBody = new char[sizeOfFiles];
            byte[] header = new byte[sizeOfheader];
            char[] content; 
            int bytesAmount = 0;
            try (
                    Socket socket = new Socket(dest.get(0).split(":")[0], Integer.parseInt(dest.get(0).split(":")[1]));
                    OutputStream outs = socket.getOutputStream();
                    InputStream ins = socket.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(ins))  
            ) {
                String outHeader = "read" + ":" + part.getName();
                header = Base64.getEncoder().encode((String.format("%-" + Integer.toString(sizeOfheader) + "s", outHeader)).getBytes());   // pad the outgoing message to 64 bytes and encode it to binary format
                outs.write(header);
                if( (bytesAmount = br.read​(filePartBody, 0, sizeOfFiles)) > 0 ) {  // read the whole chunk containing the offset
                    content = Arrays.copyOfRange(filePartBody, startPoint, bytesAmount);
                    System.out.println("File content starting from the offset:");                   
                    System.out.println(new String(content));
                    // System.out.println(Arrays.toString(filePartBody));
                }
            } catch (IOException e) {
                    System.err.print("Get chunk failed, please try again.");
                }
        }
    }

    public static void appendContent(String filename, String content) {
        if (content.length() > 1024 * 2) {  // if appended content exceeds the maximum size 2k
            System.out.println("Content exceeds the maximum size 2KB");
            return;
        }
        String dest = "";
        String partCounter = "";
        try (
            Socket socket = new Socket(Mserver, MserverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
            new InputStreamReader(socket.getInputStream()))
		) {
            out.println("request:append:" + filename + ":" + content.length());
            String[] parse = in.readLine().split(",");
            //  if the last file part is unavalibale
            if (parse.length == 1 && parse[0].equals(FNA)) {
                System.out.println(FNA);
                return;
            }
            //  if there is no such a file on server
            if (parse[0].equals(NSF)) {
                System.out.println(NSF);
                return;
            }
            dest = parse[0];
            partCounter = parse[1];
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to Meta Server.");
        }
        String filePartName = String.format("%s.%03d", filename, Integer.parseInt(partCounter));
        try (
            Socket socket = new Socket(dest.split(":")[0], Integer.parseInt(dest.split(":")[1]) );
            OutputStream outs = socket.getOutputStream()
            // BufferedReader in = new BufferedReader(new InputStreamReader(peerSocket.getInputStream())
        ) {
            String outHeader = "append" + ":" + filePartName;
            byte[] header = new byte[sizeOfheader];
            byte[] filePartBody = content.getBytes();
            header = Base64.getEncoder().encode((String.format("%-" + Integer.toString(sizeOfheader) + "s", outHeader)).getBytes());   // pad the outgoing message to 64 bytes and encode it to binary format
            if (filePartBody.length > 0) {                       
                // System.out.println(bytesAmount);
                // combine header and body
                byte[] combined = new byte[header.length + filePartBody.length];
                System.arraycopy(header,0,combined,0         ,header.length);
                System.arraycopy(filePartBody,0,combined,header.length,filePartBody.length);
                outs.write(combined, 0, header.length + filePartBody.length);
                outs.flush();
            } 
        } catch (IOException e) {
            System.err.println("Can not connect to file server.");
        }
        System.out.println("New content appended.");
    }
    public static void appendFile(String filename, File appfile) {
        if(appfile.length() > 1024 * 2) {  // if appended file exceeds the maximum size 2k
            System.out.println("appended file exceeds the maximum size 2KB");
            return;
        }
        String dest = "";
        String partCounter = "";
        byte[] filePartBody = new byte[sizeOfFiles];
        try (
            Socket socket = new Socket(Mserver, MserverPort);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
            new InputStreamReader(socket.getInputStream()))
		) {
            out.println("request:append:" + filename + ":" + Long.toString(appfile.length()));
            String[] parse = in.readLine().split(",");
            //  if the last file part is unavalibale
            if (parse.length == 1 && parse[0].equals(FNA)) {
                System.out.println(FNA);
                return;
            }
            dest = parse[0];
            partCounter = parse[1];
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to Meta Server.");
        }
        if (dest.length() > 0) {
            try (
                FileInputStream fis = new FileInputStream(appfile);
                ) {
                    String filePartName = String.format("%s.%03d", filename, Integer.parseInt(partCounter));
                    // StringBuilder outMsg = new StringBuilder(filePartName + ":");
                    int bytesAmount = 0;
                    try (
                        Socket socket = new Socket(dest.split(":")[0], Integer.parseInt(dest.split(":")[1]) );
                        OutputStream outs = socket.getOutputStream()
                        // BufferedReader in = new BufferedReader(new InputStreamReader(peerSocket.getInputStream())
                    ) {
                        String outHeader = "append" + ":" + filePartName;
                        byte[] header = new byte[sizeOfheader];
                        header = Base64.getEncoder().encode((String.format("%-" + Integer.toString(sizeOfheader) + "s", outHeader)).getBytes());   // pad the outgoing message to 64 bytes and encode it to binary format
                        if ((bytesAmount = fis.read(filePartBody)) > 0) {                       
                            // System.out.println(bytesAmount);
                            // combine header and body
                            byte[] combined = new byte[header.length + bytesAmount];
                            System.arraycopy(header,0,combined,0         ,header.length);
                            System.arraycopy(filePartBody,0,combined,header.length,bytesAmount);
                            outs.write(combined, 0, header.length + bytesAmount);
                            outs.flush();
                        } 
                    } catch (IOException e) {
                        System.err.println("Can not connect to file server.");
                    }
                } catch (IOException e) {
                System.err.println("Open file failed.");
            }
            System.out.println("New content appended.");
        } else {
            System.out.println("No server is available.");
        }
    }
    public static void main(String args[]) {

        try {
            myHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("Can not get local host name.");
        }
        
        while(true) {
            Scanner sc = new Scanner(System.in);
            System.out.println("Please choose an operation：read $filename $offset(-1 retrieves the whole file)/append $filename/create $filename/transfer $filename");
            String command = sc.next();
            String file = sc.next();
            switch (command) {
                case("read"): {
                    int offset = sc.nextInt();
                    if (offset < -1) {
                        System.out.println("offset not recogonized.");
                    } else {
                        readFile(file, offset);
                    }
                    break;
                }
                case("append"): {
                    sc.useDelimiter("EOF");
                    System.out.println("Input the content to append(max 2KB):");
                    appendContent(file, sc.next());
                    break;
                }
                // case("append"): {
                //     System.out.println("input the source file full path to append(max 2KB):");
                //     appendFile(file, new File(sc.next()));
                //     break;
                // }
                case("transfer"): {
                    System.out.println("Input the source file full path to transfer(max 8000KB):");
                    transferFile(new File(sc.next()));
                    break;
                }
                case("create"): {
                    createFile(file);
                    break;
                }
                default: {
                    System.out.println("Command not recognized.");
                }
            }
        }
    }
}
