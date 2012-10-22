import java.io.*;
import java.net.*;
import java.util.Arrays;

/*/////////////////////////////////////////////////////////////////////////////////
*	assume the client data is enough
*	setup the client_n (client number)
*	k (the top k you want)
*	port (listen port)
*	client_data_length (client datalength)
*	when the compute finish
*	you can check the result "D:/Compute Result" compare to "D:/All Data Check.txt" 
/////////////////////////////////////////////////////////////////////////////////*/
public class Server{
	final static int client_n=3;
	final static int k=10000;
	final static int client_data_length=k;
	final static int port=1990;
	public static void main(String[] args){
		Thread td[]=new Thread[client_n];
		int com=0,flow=0;							//com-communicate_count   flow(byte)
		ServerSocket ss=null;
		Socket clis[]=new Socket[client_n];			// n client socket
		int mrank[]=new int[client_n];				// client max's rank
		int topk[]=new int[k];						//final top k element
		boolean last[]=new boolean[client_n];		//buff active
		int pos[][]=new int[client_n][];			//position
		int range[][]=new int[client_n][];			//current range
		int buff[][]=new int[client_n][];			//buffer
		GetData thr[]=new GetData[client_n];		//for multi_thread data communicate
		for(int i=0;i<client_n;i++){				//initialize
			thr[i]=new Server().new GetData();
			pos[i]=new int[1];
			range[i]=new int[2];
			range[i][0]=0;
			range[i][1]=0;
			buff[i]=new int[client_data_length];
			mrank[i]=0;
			last[i]=false;
		}
		//listen
		try{
			ss=new ServerSocket(port);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		//connect
		System.out.println("Server Started ! Waiting For Connection ...");
		int c=0;
		while(c<client_n){
			try{
				clis[c++]= ss.accept();
				System.out.println(clis[c-1].getInetAddress()+" @ "+clis[c-1].getPort()+"   Is Connected !");
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		System.out.println("All Client Connected !");
		System.out.println("Computing ...");
		System.out.println("--------------------------------");

		long start=System.currentTimeMillis();
		
		for(int i=0;i<client_n;i++,com++,flow++){	
			thr[i].Sett(clis[i],buff[i],0,1);	//get max
			td[i]=new Thread(thr[i]);
			td[i].start();
		}
		for(int i=0;i<client_n;i++)		//synchronize
			while(td[i].isAlive());
	
		for(int i=0;i<client_n-1;i++)	//sort order
			for(int j=0;j<client_n-1-i;j++)
				if(buff[j][0]<buff[j+1][0]){
					int tint=buff[j][0];
					buff[j][0]=buff[j+1][0];
					buff[j+1][0]=tint;
					Socket tso=clis[j];
					clis[j]=clis[j+1];
					clis[j+1]=tso;					
				}
		
		mrank[0]=0;
		int ck=0;
		for(int i=1;i<=client_n;i++){		// main process
			if(i==client_n && ck<k){
				ck=mrank[i-1];
				heap hp=new Server().new heap(k);
				int hh[]=new int[2];
				for(int j=0;j<i;j++){		
					//////////////////
					com++;
					flow++;
					/////////////////////
					if(range[j][1] >= k)
						continue;
					thr[j].Sett(clis[j],buff[j],range[j][1],range[j][1]+1);
					td[j]=new Thread(thr[j]);
					td[j].start();
				}
				for(int j=0;j<i;j++){
					while(td[j].isAlive());
					hh[0]=buff[j][range[j][1]];
					hh[1]=j;
					hp.ins(hh);
				}
				while(ck<k){
					hp.del(hh);
					int cur=hh[1];
					last[cur]=true;
					range[cur][1]++;
					if(range[cur][1] >= k){	//drop
						ck++;
						continue;
					}
					thr[0].Sett(clis[cur],buff[cur],range[cur][1],range[cur][1]+1);
					/////////////////////
					com++;
					flow++;
					/////////////////////
					Thread tt=new Thread(thr[0]);
					tt.start();
					while(tt.isAlive());	//wait
					hh[0]=buff[cur][range[cur][1]];
					hh[1]=cur;
					hp.ins(hh);
					ck++;
				}
				break;
			}
			for(int j=0;j<i;j++){			
				/////////////////////
				com++;
				/////////////////////
				thr[j].Sett(clis[j],pos[j],buff[i][0]);
				td[j]=new Thread(thr[j]);
				td[j].start();
			}
			for(int j=0;j<i;j++){		//synchronize
				while(td[j].isAlive());
				mrank[i]+=pos[j][0];	//set rank
				range[j][0]=range[j][1];	//update range
				range[j][1]=pos[j][0];
			}
			if(mrank[i]<k){
				for(int j=0;j<i;j++){		
					//////////////////////
					com++;
					flow+=range[j][1]-range[j][0];
					////////////////////////
					thr[j].Sett(clis[j],buff[j],range[j][0],range[j][1]);	//get data
					td[j]=new Thread(thr[j]);
					td[j].start();
					last[j]=true;
				}
				for(int j=0;j<i;j++)
					while(td[j].isAlive());
				if(mrank[i]==k-1){			//over
					last[i]=true;
					break;
				}
			}
			else{
				//System.out.println("last");
				ck=mrank[i-1];
				heap hp=new Server().new heap(k);
				int hh[]=new int[2];
				for(int j=0;j<i;j++){		
					//////////////////////
					com++;
					flow+=1;
					/////////////////////
					range[j][1]=range[j][0];
					if(range[j][1] >= k)
						continue;
					thr[j].Sett(clis[j],buff[j],range[j][1],range[j][1]+1);
					td[j]=new Thread(thr[j]);
					td[j].start();
					//last[j]=true;
				}
				for(int j=0;j<i;j++){
					while(td[j].isAlive());
					hh[0]=buff[j][range[j][1]];
					hh[1]=j;
					hp.ins(hh);
				}
				while(ck<k){
					hp.del(hh);
					int cur=hh[1];
					last[cur]=true;
					range[cur][1]++;
					if(range[cur][1] >= k){	//drop
						ck++;					
						continue;
					}
					thr[0].Sett(clis[cur],buff[cur],range[cur][1],range[cur][1]+1);
					//////////////////////////
					com++;
					flow++;
					/////////////////////
					Thread tt=new Thread(thr[0]);
					tt.start();
					while(tt.isAlive());	//wait
					hh[0]=buff[cur][range[cur][1]];
					hh[1]=cur;
					hp.ins(hh);
					ck++;
				}
				break;
			}
		}
		
		//get result
		c=0;
		int sum=0;
		for(int i=0;i<client_n;i++){
			if(last[i]){
				sum+=range[i][1];
				for(int j=0;j<range[i][1];j++)
					topk[c++]=buff[i][j];
			}
			System.out.println(last[i]+" "+(i+1)+" "+range[i][1]);			
		}
		for(int i=0;i<k-1;i++)
			for(int j=0;j<k-1-i;j++)
				if(topk[j]<topk[j+1]){
					int temp=topk[j];
					topk[j]=topk[j+1];
					topk[j+1]=temp;
				}
		
		long ti=System.currentTimeMillis()-start;

		System.out.println("Some Top data---------------------------");
		
		for(int i=0;i<1000 && i<topk.length;i++){
			System.out.print(topk[i]+"  ");
			if((i%100)==99)System.out.println();
		}
		System.out.println();

		/******************************************************************************/
		try{
			FileWriter ff=new FileWriter("D:/Compute Result.txt");
			for(int i=0;i<topk.length;i++){
				ff.write(Integer.toString(topk[i]));
				ff.write("  ");
				if(i%100==99)
					ff.write("\n");
			}
			
			FileWriter fo=new FileWriter("D:/ALL Data Check.txt");
			int t[]=new int[client_n*client_data_length];
			for(int i=0;i<client_n;i++){
				thr[i].Sett(clis[i],buff[i],0,client_data_length);
				td[i]=new Thread(thr[i]);
				td[i].start();
			}
			for(int i=0;i<client_n;i++)
				while(td[i].isAlive());
			ck=0;
			for(int i=0;i<client_n;i++)
				for(int j=0;j<client_data_length;j++)
					t[ck++]=buff[i][j];
			Arrays.sort(t);
			for(int j=0,i=t.length-1;i>=t.length-k;i--,j++){
				fo.write(Integer.toString(t[i]));
				fo.write("  ");
				if(j%100==99)
					fo.write("\n");
			}
			
			fo.flush();
			fo.close();
			ff.flush();
			ff.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		/**********************************************************************************/	
		//over
		for(int i=0;i<client_n;i++){
			thr[i].Sett(clis[i],0);	// send 0 to terminate
			td[i]=new Thread(thr[i]);
			td[i].start();
		}
		for(int i=0;i<client_n;i++)	//wait
			while(td[i].isAlive());
		System.out.println("--------------------------------");
		System.out.println("Compute Complete !");
		System.out.println("Runnin' Time:  "+ti+"   ms");
		System.out.println("total: "+sum+"     communication_count:  "+com+"   bute_flow:  "+flow);
		System.out.println("The Result is @ \"D:/Compute Result\"");
		try{
			ss.close();
			for(Socket soso:clis)
				soso.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	class GetData implements Runnable{
		Socket sock=null;
		InputStream in=null;
		OutputStream out=null;
		int key=1;
		int st=0;
		int ed=-1;
		int pos[]=null;
		int gd[]=null;
		int msg=2;
		public void Sett(Socket s,int g[],int sta,int end){	//get from rank [start] to [end]
			msg=2;
			sock=s;
			gd=g;
			st=sta;
			ed=end;			
		}		
		public void Sett(Socket s,int p[],int k){			//locate  k's position 
			msg=1;
			sock=s;
			pos=p;		
			key=k;
		}
		public void Sett(Socket s,int m){					//terminate
			sock=s;
			msg=0;
		}
		public void run(){
			try{
				in=sock.getInputStream();
				out=sock.getOutputStream();
				byte bint[]=new byte[4];
				bint=Server.int2byte(msg);
				out.write(bint,0,4);	//header info
				if(0==msg)
					return;
				
				if(1==msg){				//locate 
					bint=Server.int2byte(key);
					out.write(bint,0,4);	//key
					in.read(bint,0,4);
					pos[0]=Server.byte2int(bint);
				}
				if(2==msg){				//get data
					bint=Server.int2byte(st);
					out.write(bint,0,4);
					bint=Server.int2byte(ed);
					out.write(bint,0,4);
					for(int i=st;i<ed;i++){
						in.read(bint,0,4);
						gd[i]=Server.byte2int(bint);
					}
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}		
			finally{		
			}
			
		}
	}
	class heap{				//max_heap
		int h[][],n;
		heap(int l){
			h=new int[l][];
			for(int i=0;i<l;i++)
				h[i]=new int[2];
			n=0;
		}
		void ins(int e[]){
			int p;
			for (p=++n;p>1&& e[0] > h[p>>1][0];h[p][0]=h[p>>1][0],h[p][1]=h[p>>1][1],p>>=1);
			h[p][0]=e[0];
			h[p][1]=e[1];
		}
		void del(int e[]){
			if (0==n) return ;
			int p,c;
			for (e[0]=h[p=1][0],e[1]=h[1][1],c=2;c<n&& h[c+=(c<n-1&& h[c+1][0] > h[c][0])?1:0][0] > h[n][0];h[p][0]=h[c][0],h[p][1]=h[c][1],p=c,c<<=1);
			h[p][0]=h[n][0];
			h[p][1]=h[n--][1];
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