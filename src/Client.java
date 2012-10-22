import java.io.*;
import java.net.*;
import java.util.*;
/*////////////////////////////////////////////////
 before Start Setup the sip & port(Server IP & port)
 datalength (Data to generate)
/////////////////////////////////////////////////*/
public class Client {
	final static int port=1990;
	final static String sip="127.0.0.1";
	final static int datalength=10000;
	public static void main(String[] args){
		Socket cli=null;
		InputStream in=null;
		OutputStream out=null;
		byte bytm[]=new byte[4];
		int type=0;
		int key=0;
		int st;
		int ed;
		int data[]=new int[datalength];
		Random grt=new Random();
		int r=grt.nextInt(9)+1;				//random r from 1-9
		for(int i=0;i<datalength;i++){
			data[i]=(int)((grt.nextGaussian()+4)*1000*r);  	//generate  Gauss random data
		}
		for(int i=0;i<datalength-1;i++)	//sort descending
			for(int j=0;j<datalength-1-i;j++)
				if(data[j]<data[j+1]){
					int temp=data[j];
					data[j]=data[j+1];
					data[j+1]=temp;
				}
		//print the data
		System.out.println("Some Top data");
		for(int i=datalength-1;i>=0;i--){
			System.out.println(data[i]);
			if(i== datalength-100)
				break;
		}
		
		try{
			cli=new Socket(sip,port);
			in=cli.getInputStream();
			out=cli.getOutputStream();
			System.out.println("--------------------------");
			System.out.println("Data Generate Complete !");
			System.out.println("Computing ...");
			System.out.println("--------------------------");
			while(true){		
				in.read(bytm,0,4);
				type=byte2int(bytm);
				if(0==type)
					break;
				
				if(1==type){					//locate key's position
					in.read(bytm,0,4);
					key=byte2int(bytm);
					int pos=0;
					for(;pos<datalength && data[pos]>=key ;pos++);
					//System.out.println("pos: "+key+" @ "+pos);
					bytm=int2byte(pos);	
					out.write(bytm,0,4);
				}
				if(2==type){					// send data from [start] to [end]
					in.read(bytm,0,4);
					st=byte2int(bytm);
					in.read(bytm,0,4);
					ed=byte2int(bytm);
					//System.out.println("from "+st+"  to  "+ed);

					for(int i=st;i<ed && i<datalength;i++){
						bytm=int2byte(data[i]);
						out.write(bytm,0,4);
					}
					if(ed>datalength)
						System.out.println("Data Error");
				}
			}
			System.out.println("Distributed Compute Complete !");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			try{
				in.close();
				out.close();
				cli.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}		
	}
	public static byte[] int2byte(int res) {
		byte[] targets = new byte[4];
		targets[0] = (byte) (res & 0xff);			// 最低位 
		targets[1] = (byte) ((res >> 8) & 0xff);	// 次低位 
		targets[2] = (byte) ((res >> 16) & 0xff);	// 次高位 
		targets[3] = (byte) (res >>> 24);			// 最高位,无符号右移。 
		return targets; 
	} 

	public static int byte2int(byte[] res) { 
		// byte[4] to int
		int targets = (res[0] & 0xff) | ((res[1] << 8) & 0xff00) | ((res[2] << 24) >>> 8) | (res[3] << 24); 
		return targets; 
	}
}
