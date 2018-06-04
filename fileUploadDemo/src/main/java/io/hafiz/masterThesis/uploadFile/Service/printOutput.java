package io.hafiz.masterThesis.uploadFile.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class printOutput extends Thread {
	InputStream input_stream = null;
	
	printOutput(InputStream input_stream, String type){
        this.input_stream = input_stream;
    }
		
   public void run(){
       String s = null;
       try{
           BufferedReader br = new BufferedReader(new InputStreamReader(input_stream));
           while((s = br.readLine()) != null){
               System.out.println(s);
           }
       }catch (IOException ioe){
           ioe.printStackTrace();
       }
   }
}
