package hw4;

import java.io.*; 
import java.net.*; 
  
class server { 
	 static  int count=0;
	 static int num=1;
	 static  int pack=0;
	 static   long startTime;
  public static void main(String args[]) throws Exception 

    { 
     try
     { 
    	 BufferedWriter writer=null;
 		try {
 			writer = new BufferedWriter(new FileWriter("/Users/vaishnavisabhahith/Desktop/Test/output/out.csv"));
 		} catch (FileNotFoundException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} 
    	
      DatagramSocket serverSocket = new DatagramSocket(9876); 
  
      byte[] receiveData = new byte[1024]; 
      byte[] sendData  = new byte[1024]; 
  
      while(true) 
        { 
    	 
          receiveData = new byte[1024]; 

          DatagramPacket receivePacket = 
             new DatagramPacket(receiveData, receiveData.length); 


          serverSocket.receive(receivePacket); 

          String sentence = new String(receivePacket.getData()); 
  
          InetAddress IPAddress = receivePacket.getAddress(); 
  
          int port = receivePacket.getPort(); 
  
 count++;
 pack++;
 System.out.print("packets"+pack);
          
          if(count == 10000){
        	  try {
        		  writer.close();
        		  System.out.println("num"+num);
        		  String filename = "/Users/vaishnavisabhahith/Desktop/Test/output/"+"out"+num+".csv";
      			writer = new BufferedWriter(new FileWriter(filename));
      			count = 0;
      			num++;
      		} catch (FileNotFoundException e) {
      			// TODO Auto-generated catch block
      			e.printStackTrace();
      		}
          }
          writer.write(sentence);
          
          String capitalizedSentence = sentence.toUpperCase(); 

          sendData = capitalizedSentence.getBytes(); 
  
          DatagramPacket sendPacket = 
             new DatagramPacket(sendData, sendData.length, IPAddress, 
                               port); 
  
          serverSocket.send(sendPacket); 

        } 
      
     }
      catch (SocketException ex) {
        System.out.println("UDP Port 9876 is occupied.");
        System.exit(1);
      }

    } 
}  
