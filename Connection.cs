﻿using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace DGPGrid
{    
	// State object for reading client data asynchronously
	public class StateObject
	{
		// Client socket.
		public Socket workSocket = null;
		// Size of receive buffer.
		public const int BufferSize = 16384;
		// Receive buffer.
		public byte[] buffer = new byte[BufferSize];
		// Received data string.
		public bool received = false;
	}
    
    public class MessageProcessor
    {
        private enum ConnectionState
        {
            Idle = 0,
            Receiving,
            ReceivingPeers,
            ReceivingRoute,
            ReceivingTasks,
            ReceivingExec,
            ReceivingData,
            ReceivingRes,
            Sending,
            Ping
        };
        
    	public Connection con = null;
        private volatile ConnectionState state;
        public bool peerChanged = true;
        public bool taskChanged = true;
        public bool routeChanged = true;
        public int dataSent = 0;
        public int dataReceived = 0;
        public int dataWindow = 5000;
        
        
		#region Commands
		byte[] cComplete = { 0 };
		byte[] cReceived = { 1 };
		byte[] cPing = { 2 };
		byte[] cSendId = { 3 };
		byte[] cSendRes = { 4 };
		byte[] cSendPeer = { 5 };
		byte[] cSendTask = { 6 };
		byte[] cSendExec = { 7 };
		byte[] cSendData = { 8 };
		byte[] cSendRoute = { 9 };
		byte[] cAskExec = { 10 };
		#endregion
		byte[] ReceivedData = null;
		int index = 0;
		int toGet = 0;
		int len = 0;
		int minPackSize = 0;
		string askedFor = "";
		string jobID = "";
		string dataPath = "";
		Stream file = null;
			
    	public MessageProcessor(Connection connect)
    	{
    		state = ConnectionState.Idle;
    		con = connect;
    		SendID();
    	}
    	
    	public void Parse(byte[] input, int size)
    	{
    		//Console.WriteLine(BitConverter.ToString(input, 0, size).Replace("-","") + " Size: " + size.ToString());
    		
			if (input[0] == cReceived[0] && state == ConnectionState.Sending)
			{
				state = ConnectionState.Idle;
			}    		
			
    		#region ReceiveID
			if (input[0] == cSendId[0] && state == ConnectionState.Idle)
			{
				int length = BitConverter.ToInt32(input, 1);
				con.ID = Encoding.ASCII.GetString(input, sizeof(Int32)+1, length);
			}    		
    		#endregion
    		
    		#region ReceiveRoutes
			if (input[0] == cSendRoute[0] && state == ConnectionState.Idle)
			{
				int length = BitConverter.ToInt32(input, 1);
				ReceivedData = new byte[length];
				if(length<1)
					return;
				if(size<length)
				{
					len = length;
					toGet = len - size + sizeof(Int32) + 1;
					Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, size-1-sizeof(Int32));
					index = size-1-sizeof(Int32);
					state = ConnectionState.ReceivingRoute;
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, length);
				
				string Routes = Encoding.ASCII.GetString(ReceivedData, 0, length);
				
				DataBase.MergeRoutes(Routes, con.ID);
				
				con.Send(cReceived);
			}
			
			if(state == ConnectionState.ReceivingRoute)
			{
				if(size<toGet)
				{
					Array.Copy(input, 0, ReceivedData, index, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, ReceivedData, index, toGet);
					index += toGet;
					toGet -= toGet;
				}
				if(toGet == 0)
				{
					
					string Routes = Encoding.ASCII.GetString(ReceivedData, 0, len);
					
					DataBase.MergeRoutes(Routes, con.ID);
					con.Send(cReceived);
					state = ConnectionState.Idle;
				}
				
			}
    		#endregion
    		
    		#region ReceivePeer
			if (input[0] == cSendPeer[0] && state == ConnectionState.Idle)
			{
				int length = BitConverter.ToInt32(input, 1);
				ReceivedData = new byte[length];
				if(length<1)
					return;
				if(size<length)
				{
					len = length;
					toGet = len - size + sizeof(Int32) + 1;
					Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, size-1-sizeof(Int32));
					index = size-1-sizeof(Int32);
					state = ConnectionState.ReceivingPeers;
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, length);
		    	MemoryStream memStream = new MemoryStream();
       	 		memStream.Write(ReceivedData, 0, length);
       	 		memStream.Seek(0, SeekOrigin.Begin);
       	 		
			    System.Xml.Serialization.XmlSerializer s = 
			        new System.Xml.Serialization.XmlSerializer(typeof(List<PeerEntry>));
				List<PeerEntry> inpeers = (List<PeerEntry>)s.Deserialize(memStream);
				
				DataBase.MergePeers(inpeers);
				
				con.Send(cReceived);
			}
			
			if(state == ConnectionState.ReceivingPeers)
			{
				if(size<toGet)
				{
					Array.Copy(input, 0, ReceivedData, index, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, ReceivedData, index, toGet);
					index += toGet;
					toGet -= toGet;
				}
				if(toGet == 0)
				{
			    	MemoryStream memStream = new MemoryStream();
	       	 		memStream.Write(ReceivedData, 0, len);
	       	 		memStream.Seek(0, SeekOrigin.Begin);
			    	System.Xml.Serialization.XmlSerializer s = 
			    	    new System.Xml.Serialization.XmlSerializer(typeof(List<PeerEntry>));
					List<PeerEntry> inpeers = (List<PeerEntry>)s.Deserialize(memStream);
					DataBase.MergePeers(inpeers);
					con.Send(cReceived);
					state = ConnectionState.Idle;
				}
				
			}
    		#endregion
    		
    		#region ReceiveTask
			if (input[0] == cSendTask[0] && state == ConnectionState.Idle)
			{
				int length = BitConverter.ToInt32(input, 1);
				ReceivedData = new byte[length];
				if(length<1)
					return;
				if(size<length)
				{
					len = length;
					toGet = len - size + sizeof(Int32) + 1;
					Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, size-1-sizeof(Int32));
					index = size-1-sizeof(Int32);
					state = ConnectionState.ReceivingTasks;
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, length);
		    	MemoryStream memStream = new MemoryStream();
       	 		memStream.Write(ReceivedData, 0, length);
       	 		memStream.Seek(0, SeekOrigin.Begin);
       	 		
			    System.Xml.Serialization.XmlSerializer s = 
			        new System.Xml.Serialization.XmlSerializer(typeof(List<TaskEntry>));
				List<TaskEntry> injobs = (List<TaskEntry>)s.Deserialize(memStream);
				
				DataBase.MergeTasks(injobs);
				
				con.Send(cReceived);
			}
			
			if(state == ConnectionState.ReceivingTasks)
			{
				if(size<toGet)
				{
					Array.Copy(input, 0, ReceivedData, index, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, ReceivedData, index, toGet);
					index += toGet;
					toGet -= toGet;
				}
				if(toGet == 0)
				{
			    	MemoryStream memStream = new MemoryStream();
	       	 		memStream.Write(ReceivedData, 0, len);
	       	 		memStream.Seek(0, SeekOrigin.Begin);
			    	System.Xml.Serialization.XmlSerializer s = 
			    	    new System.Xml.Serialization.XmlSerializer(typeof(List<TaskEntry>));
					List<TaskEntry> injobs = (List<TaskEntry>)s.Deserialize(memStream);
					DataBase.MergeTasks(injobs);
					con.Send(cReceived);
					state = ConnectionState.Idle;
				}
				
			}
    		#endregion
    		
    		#region ReceiveExec
			if (input[0] == cSendExec[0] && state == ConnectionState.Idle)
			{
				if(File.Exists(Settings.workingDirectory + askedFor + "\\task.exe"))
				{
					return;
				}
				
				int length = BitConverter.ToInt32(input, 1);
				ReceivedData = new byte[length];
				if(length<1)
					return;
				if(size<length)
				{
					len = length;
					toGet = len - size + sizeof(Int32) + 1;
					Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, size-1-sizeof(Int32));
					index = size-1-sizeof(Int32);
					state = ConnectionState.ReceivingExec;
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, length);
				
				if(!Directory.Exists(Settings.workingDirectory + askedFor))
					Directory.CreateDirectory(Settings.workingDirectory + askedFor);
				
				Stream exec = File.OpenWrite(Settings.workingDirectory + askedFor + "\\task.exe");
				
       	 		exec.Write(ReceivedData, 0, length);
				
				con.Send(cReceived);
				askedFor = "";
			}
			
			if(state == ConnectionState.ReceivingExec)
			{
				if(File.Exists(Settings.workingDirectory + askedFor + "\\task.exe"))
				{
					return;
				}
				if(size<toGet)
				{
					Array.Copy(input, 0, ReceivedData, index, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, ReceivedData, index, toGet);
					index += toGet;
					toGet -= toGet;
				}
				if(toGet == 0)
				{
				
					if(!Directory.Exists(Settings.workingDirectory + askedFor))
						Directory.CreateDirectory(Settings.workingDirectory + askedFor);
					
					Stream exec = File.OpenWrite(Settings.workingDirectory + askedFor + "\\task.exe");
					
	       	 		exec.Write(ReceivedData, 0, len);
					
					con.Send(cReceived);
					state = ConnectionState.Idle;
					askedFor = "";
				}
				
			}
    		#endregion
    		    		
    		#region ReceiveData
			if (input[0] == cSendData[0] && state == ConnectionState.Idle)
			{
				int inIndex = 1;
				minPackSize = 1 + sizeof(int);
					
				int jobIDLen = BitConverter.ToInt32(input, inIndex);
								
				jobID = Encoding.ASCII.GetString(input, sizeof(Int32)+1, jobIDLen);
				inIndex += sizeof(Int32) + jobIDLen;
				int dataPathLen = BitConverter.ToInt32(input, inIndex);
				//Console.WriteLine("Packet size {0}, job Length {1}, Path Len {2}", size.ToString(), jobIDLen.ToString(), dataPathLen.ToString());
				dataPath = Encoding.ASCII.GetString(input, sizeof(Int32)+inIndex, dataPathLen);
				inIndex += sizeof(Int32) + dataPathLen;
        	
				int length = BitConverter.ToInt32(input, inIndex);
				inIndex += sizeof(Int32);
				ReceivedData = new byte[length];
				
				//Console.WriteLine("Getting {0} from job {1}", dataPath, jobID);
				
				
				if(length<1)
					return;
				if(size<length)
				{
					state = ConnectionState.ReceivingData;
				//	file = File.OpenWrite(Settings.workingDirectory + jobID + "\\" + dataPath);
					len = length;
					toGet = len - size + inIndex;
					Array.Copy(input, inIndex, ReceivedData, 0, size-inIndex);
       	 			//file.Write(input, inIndex, size-inIndex);
					index = size-inIndex;
					
					return;
				}
				Array.Copy(input, inIndex, ReceivedData, 0, length);
				
				
					lock(DataBase.dataTrans)
					{
						System.Diagnostics.Debug.WriteLine("added {0} = {1}", dataPath, Settings.myID);
			   			DataBase.dataTrans[dataPath] = Settings.myID;
					}
				
				if(!Directory.Exists(Settings.workingDirectory + jobID + "\\data"))
					Directory.CreateDirectory(Settings.workingDirectory + jobID + "\\data");
				
				
       	 		file.Write(ReceivedData, 0, length);
       	 		//file.Write(input, inIndex, length);
				file.Close();
				
				con.Send(cReceived);
					state = ConnectionState.Idle;
			}
			
			if(state == ConnectionState.ReceivingData)
			{
				if(size<toGet)
				{
					Array.Copy(input, 0, ReceivedData, index, size);
       	 			//file.Write(input, 0, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, ReceivedData, index, toGet);
       	 			//file.Write(input, 0, toGet);
					index += toGet;
					toGet -= toGet;
				}
				//Console.WriteLine("Len: {0}, Index: {1}, toGet: {2}", len, index, toGet);
				if(toGet == 0)
				{
				
					if(!Directory.Exists(Settings.workingDirectory + jobID + "\\data"))
						Directory.CreateDirectory(Settings.workingDirectory + jobID + "\\data");
					lock(DataBase.dataTrans)
					{
						System.Diagnostics.Debug.WriteLine("added {0} = {1}", dataPath, Settings.myID);
			   			DataBase.dataTrans[dataPath] = Settings.myID;
					}
				
					Stream exec = File.OpenWrite(Settings.workingDirectory + jobID + "\\" + dataPath);
					
	       	 		exec.Write(ReceivedData, 0, len);
					
					con.Send(cReceived);
					exec.Close();
					//file.Close();
					state = ConnectionState.Idle;
				}
				
			}
    		#endregion
    		
    		#region AskedForExec
			if (input[0] == cAskExec[0]  && state == ConnectionState.Idle)
			{
				int length = BitConverter.ToInt32(input, 1);
				
				if(size < (length + 1 + sizeof(int)))
					return;
				
				try{
					string jobID = Encoding.ASCII.GetString(input, sizeof(Int32)+1, length);
					SendExec(jobID);
				}
				catch(Exception e)
				{
					System.Diagnostics.Debug.WriteLine(e.ToString());
				}
			}
    		#endregion
    		
			if (input[0] == cPing[0]  && state == ConnectionState.Idle)
			{
				Console.WriteLine("Ping");
				con.Send(cReceived);
			}
			
			if(state == ConnectionState.Ping)
			{
				if (input[0] == cReceived[0])
				{
					Console.WriteLine("Pong");
					state = ConnectionState.Idle;
				}
			}
    	}
    	
    	public void SendPeer()
    	{
			
    		byte[] msg = cSendPeer;
    		char[] ch = null;
    		
		    System.Xml.Serialization.XmlSerializer s = 
		        new System.Xml.Serialization.XmlSerializer(typeof(List<PeerEntry>));
		    MemoryStream stream = new MemoryStream();
			
			s.Serialize(stream, DataBase.peers);
       	 	var sr = new StreamReader(stream);
       	 	stream.Position = 0;
        	ch = new char[stream.Length];
        	sr.Read(ch, 0, (int)stream.Length);
        	byte[] dataLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes(ch).Length);
        	byte[] data = Encoding.ASCII.GetBytes(ch);
        	byte[] message = new byte[msg.Length + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
			stream.Close();
    	}
    	
    	public void SendTask()
    	{
			
    		byte[] msg = cSendTask;
    		char[] ch = null;
    		
		    System.Xml.Serialization.XmlSerializer s = 
		        new System.Xml.Serialization.XmlSerializer(typeof(List<TaskEntry>));
		    MemoryStream stream = new MemoryStream();
			
		    s.Serialize(stream, DataBase.jobs);
       	 	var sr = new StreamReader(stream);
       	 	stream.Position = 0;
        	ch = new char[stream.Length];
        	sr.Read(ch, 0, (int)stream.Length);
        	byte[] dataLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes(ch).Length);
        	byte[] data = Encoding.ASCII.GetBytes(ch);
        	byte[] message = new byte[msg.Length + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
			stream.Close();
    	}
    	
    	public void SendID()
    	{
    		byte[] msg = cSendId;
    		byte[] data = Encoding.ASCII.GetBytes(Settings.myID);
        	byte[] dataLen = BitConverter.GetBytes(data.Length);
        	byte[] message = new byte[msg.Length + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
    	}
    	
    	public void SendRoute()
    	{
    		byte[] msg = cSendRoute;
    		string sRoute = "";
    		
    		foreach(TaskEntry job in DataBase.jobs)
    		{
    			int metric = -1;
    			foreach(Route KV in job.routes)
    			{
    				if(KV.cons.Count() == 1)
    					if(KV.cons[0] == con.ID)
    					  continue;
    				if(KV.metric < metric || metric == -1)
    					metric = KV.metric;
    			}
    			sRoute += metric + "|" + job.id + "|";
    		}
    		
    		byte[] data = Encoding.ASCII.GetBytes(sRoute);
    		byte[] dataLen = BitConverter.GetBytes(data.Length);
        	byte[] message = new byte[msg.Length + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
    	}
    	
    	public void AskExec(string jobID)
    	{
    		byte[] msg = cAskExec;
    		if(String.IsNullOrEmpty(jobID))
    			return;
    		askedFor = jobID;
						
        	byte[] data = Encoding.ASCII.GetBytes(jobID);
        	byte[] dataLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes(jobID).Length);
        	byte[] message = new byte[msg.Length + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
    	}
    	
    	public void SendExec(string jobID)
    	{
    		byte[] msg = cSendExec;
			Stream exec = File.OpenRead(Settings.workingDirectory + jobID + "\\task.exe");
						
			int execFileLength = (int)exec.Length;
			byte[] dataLen = BitConverter.GetBytes(execFileLength);
			byte[] data = new byte[exec.Length];
			exec.Read(data, 0, data.Length);
			
        	byte[] message = new byte[msg.Length + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
    	}
    	
    	public void SendData(string injobID, string indataPath)
    	{
			Console.WriteLine("Sent {0} = {1}", indataPath, con.ID);
    		byte[] msg = cSendData;
			Stream dataFile = File.OpenRead(Settings.workingDirectory + injobID + "\\" + indataPath);
						
			byte[] jobID = Encoding.ASCII.GetBytes(injobID);
        	byte[] jobIDLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes(injobID).Length);
        	
			byte[] dataPath = Encoding.ASCII.GetBytes(indataPath);
        	byte[] dataPathLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes(indataPath).Length);
        	
			byte[] data = new byte[dataFile.Length];
			int dataFileLength = (int)dataFile.Length;
        	byte[] dataLen = BitConverter.GetBytes(dataFileLength);
			dataFile.Read(data, 0, data.Length);
			
        	byte[] message =
        		new byte[msg.Length + jobID.Length + jobIDLen.Length + dataPath.Length + dataPathLen.Length
        		         + dataLen.Length + data.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(jobIDLen, 0, message, index, jobIDLen.Count());
        	index += jobIDLen.Count();
        	Array.Copy(jobID, 0, message, index, jobID.Count());
        	index += jobID.Count();
        	Array.Copy(dataPathLen, 0, message, index, dataPathLen.Count());
        	index += dataPathLen.Count();
        	Array.Copy(dataPath, 0, message, index, dataPath.Count());
        	index += dataPath.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	con.Send(message);
        	dataFile.Close();
    	}
    	
    	public void MessageSending()
    	{
    		while(true)
    		{
    			Thread.Sleep(1000);
    			if (state == ConnectionState.Ping)
    			{
    				Console.WriteLine("Ping have not been answered");
    				con.Dispose();
    				continue;
    			}
    			if (state == ConnectionState.Idle)
    			{
    				if(peerChanged)
    				{
    					SendPeer();
    					peerChanged = false;
    				}
    				else if(taskChanged)
    				{
    					SendTask();
    					taskChanged = false;
    				}
    				else if(routeChanged)
    				{
    					SendRoute();
    					routeChanged = false;
    				}
    				else if(!String.IsNullOrEmpty(askedFor) && !File.Exists(Settings.workingDirectory + askedFor + "\\task.exe"))
    				{
    					TaskManager.AskForExec(askedFor);
    				}
    				else
    				{
    					if(dataSent - dataReceived > dataWindow)
    						continue;
			    			//Console.WriteLine("New round");
    					string Key = "";
    					//Dictionary<string, string> temp = new Dictionary<string, string>();
    					lock(DataBase.dataTrans)
    					{
				    		foreach(KeyValuePair<string, string> KV in DataBase.dataTrans)
				    		{
				    			if(KV.Value == Settings.myID && String.Compare(con.ID, "KrylCW") != 0 && !TaskManager.pool.Contains(KV.Key))
				    			{
				    				SendData("1", KV.Key);
				    				dataSent++;
				    				//temp.Add(KV.Key, con.ID);
				    				Key = KV.Key;
				    				break;
				    			}
				    		}
				    		if(!String.IsNullOrEmpty(Key))
				    		{
				    			DataBase.dataTrans[Key] = con.ID;
				    		}
    					}
			    		//temp.Clear();
						//state = ConnectionState.Ping;
    					//con.Send(cPing);
    				}
    				continue;
    			}
    		}
    	}
    }
    
    public class Connection
    {
    	public Socket handle;
    	public StateObject state;
    	public string ID;
    	public MessageProcessor mp = null;
    	
        private System.Threading.Semaphore m_semaphore = new System.Threading.Semaphore(1, 1);
    	
    	public Connection( Socket inHandle, StateObject inState)
    	{
    		state = inState;
    		handle = inHandle;
    		
    		Console.WriteLine("Started new connection with {0}", handle.RemoteEndPoint.ToString());
    		
            m_semaphore.WaitOne();
    		DataBase.AddConn(handle.RemoteEndPoint.ToString(), this);
            m_semaphore.Release();
            mp = new MessageProcessor(this);
            Task.Factory.StartNew(() => mp.MessageSending());
            
    		handle.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0, new AsyncCallback(ReadCallback), state);
    	}
    	
		public void ReadCallback(IAsyncResult ar)
		{
			String content = String.Empty;
			
			// Retrieve the state object and the handler socket
			// from the asynchronous state object.
			StateObject state = (StateObject)ar.AsyncState;
			Socket handler = state.workSocket;
			
			// Read data from the client socket.
			try{
				int bytesRead = handler.EndReceive(ar);
				
				
				if (bytesRead > 0)
				{
						mp.Parse(state.buffer, bytesRead);
					handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0, new AsyncCallback(ReadCallback), state);
				}
			}
			catch(SocketException e)
			{
				if(String.Equals(e.SocketErrorCode.ToString(), "ConnectionReset"))
				{
	            	m_semaphore.WaitOne();
    				Console.WriteLine("Connection with {0} lost", handle.RemoteEndPoint.ToString());
					DataBase.DelConn(handler.RemoteEndPoint.ToString());
	                m_semaphore.Release();			
				}
				else
					throw(e);
			}
		}
		
		public void Send(byte[] data)
		{
			// Begin sending the data to the remote device.
			handle.BeginSend(data, 0, data.Length, 0, new AsyncCallback(SendCallback), handle);
		}
			
		private void SendCallback(IAsyncResult ar)
		{
			try
			{
				// Retrieve the socket from the state object.
				Socket handler = (Socket)ar.AsyncState;
				
				// Complete sending the data to the remote device.
				int bytesSent = handler.EndSend(ar);
				System.Diagnostics.Debug.WriteLine("Sent {0} bytes to client.", bytesSent);
							
			}
			catch (Exception e)
			{
				System.Diagnostics.Debug.WriteLine(e.ToString());
			}
		}
		public void Dispose()
		{
			//handle.Shutdown(SocketShutdown.Both);
			//handle.Close();
    		DataBase.DelConn(handle.RemoteEndPoint.ToString());
		}
    	public void setPeerChanged()
    	{
    		mp.peerChanged = true;
    	}
    	public void setTaskChanged()
    	{
    		mp.taskChanged = true;
    	} 
    	public void setRouteChanged()
    	{
    		mp.routeChanged = true;
    	}
    	public void AskForExec(string jobID)
    	{
    		mp.AskExec(jobID);
    	}
    }   
}