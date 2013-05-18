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
    	public Connection con = null;
        public bool peerChanged = true;
        public bool taskChanged = true;
        public bool routeChanged = true;
        public bool Sending = false, Receiving = false;
        public int dataSent = 0;
        public int dataReceived = 0;
        public int dataWindow = 10;
        
        public enum ConnectionStatus
        {
            Idle = 0,
            Receiving,
            ReceivingPeers,
            ReceivingRoute,
            ReceivingTasks,
            ReceivingExec,
            ReceivingData,
            ReceivingResul,
            Sending,
            Ping
        };
        
        public volatile ConnectionStatus status;
        
		public enum Command : byte
		{
			Complete = 0,
			Received,
			Ping,
			AskExec,
			AskData,
			AskResult,
			SendID,
			SendResult,
			SendPeer,
			SendTask,
			SendExec,
			SendData,
			SendRoute
		}
		
		byte[] data = null;
		int dataSize = 0;
		int dataIndex = 0;
		int dataRemainder = 0;
		
		#region Commands
		byte[] cComplete = { 0 };
		byte[] cReceived = { 1 };
		byte[] cPing = { 2 };
		byte[] cSendID = { 3 };
		byte[] cSendRes = { 4 };
		byte[] cSendPeer = { 5 };
		byte[] cSendTask = { 6 };
		byte[] cSendExec = { 7 };
		byte[] cSendData = { 8 };
		byte[] cSendRoute = { 9 };
		byte[] cAskExec = { 10 };
		byte[] cAskData = { 11 };
		byte[] cAskRes = { 12 };
		#endregion
		byte[] ReceivedData = null;
		int index = 0;
		int toGet = 0;
		int len = 0;
		string askedFor = "";
		string jobID = "";
		string dataPath = "";
		Stream file = null;
			
    	public MessageProcessor(Connection connect)
    	{
    		status = ConnectionStatus.Idle;
    		con = connect;
    		SendID();
    	}
    	
    	public void getData(byte[] input, int size, int index = 0)
    	{    		
    		if(Receiving)
    		{
    			Console.WriteLine("S: {1}; I: {0}; R: {2}; G: {3}", dataIndex.ToString(), dataSize.ToString(), dataRemainder.ToString(), size.ToString());
	    		if(size < dataRemainder)
				{
					Array.Copy(input, 0, data, dataIndex, size);
					dataIndex += size;
					dataRemainder -= size;
					return;
				}
	    		
	    		if(size == dataRemainder)
				{
					Array.Copy(input, 0, data, dataIndex, dataRemainder);
					Receiving = false;
					tryParse(data, dataSize);
					return;
				}
	    		
	    		if(size > dataRemainder)
				{
					Array.Copy(input, 0, data, index, dataRemainder);
					Receiving = false;
					tryParse(data, dataSize);
					getData(input, size - dataRemainder, dataRemainder);
					return;
				}
    		}
    		else
    		{
    			dataSize = BitConverter.ToInt32(input, index);
    			Console.WriteLine("Rec payload: {0}", dataSize.ToString());
				data = new byte[dataSize];
	    		index += sizeof(int);
				
	    		if(size - index < dataSize)
	    		{
	    			Receiving = true;
					dataIndex = size - index;
					Array.Copy(input, index, data, 0, dataIndex);
					dataRemainder = dataSize - dataIndex;
					return;
	    		}
	    		
	    		if(size - index  == dataSize)
	    		{
					Array.Copy(input, index, data, 0, dataSize);
					tryParse(data, dataSize);
					return;
	    		}
	    		
	    		if(size - index > dataSize)
	    		{
					Array.Copy(input, index, data, 0, dataSize);
					tryParse(data, dataSize);
					getData(input, size - index - dataSize, index + dataSize);
					return;
	    		}
    		}
    	}
    	
    	public void tryParse(byte[] input, int size)
    	{
    		int index = 1, partSize = 0;
		
    		List<byte[]> parts = new List<byte[]>();
    		
    		while(index != size)
    		{
    			partSize = BitConverter.ToInt32(input, index);
    			index += sizeof(int);
    			
    			parts.Add(new byte[partSize]);
    			Console.WriteLine("Size: {0}", partSize.ToString());
    			Array.Copy(input, index, parts.Last(), 0, partSize);
    			//Console.WriteLine("Data {0}", Encoding.ASCII.GetString(parts.Last()));
    			index += partSize;
    		}
    		
    		if(input[0] == cSendID[0]) 		{ GetID(parts[0]); return; }
    		if(input[0] == cSendPeer[0]) 	{ GetPeer(parts[0]); return; }
    		if(input[0] == cSendTask[0]) 	{ GetTask(parts[0]); return; }
    		if(input[0] == cSendData[0]) 	{ GetData(parts[0], parts[1], parts[2]); return; }
    	}
    	
    	public byte[] formMessage( List<byte[]> inputParts )
    	{
    		int size = 0;
        	int index = 0;
    		bool first = true;
    		
    		foreach(byte[] part in inputParts)
    		{
    			size += part.Length + sizeof(int);
    			//Console.WriteLine("payload size: {0}", size.ToString());
    		}
    		
    		byte[] message = new byte[size];
    		size -= sizeof(int);
    		
    		foreach(byte[] part in inputParts)
    		{
    			if(first)
    			{
    				Array.Copy(BitConverter.GetBytes((int)size), 0, message, index, BitConverter.GetBytes((int)size).Count());
        			index += (int)BitConverter.GetBytes((int)size).Count();
    				first = false;
    			}
    			else
    			{
    				Array.Copy(BitConverter.GetBytes((int)part.Count()), 0, message, index, BitConverter.GetBytes((int)part.Count()).Count());
        			index += BitConverter.GetBytes((int)part.Count()).Count();
    				Console.WriteLine("part size {0}", part.Count().ToString());
    				
    			}
	        	Array.Copy(part, 0, message, index, (int)part.Count());
	        	index += (int)part.Count();
    		}
    		
    		return message;
    	}
    	
    	public byte[] Serialize(Type objType, object Obj)
    	{    		
		    System.Xml.Serialization.XmlSerializer s = 
		        new System.Xml.Serialization.XmlSerializer(objType);
		    MemoryStream stream = new MemoryStream();
			
			s.Serialize(stream, Obj);
       	 	var sr = new StreamReader(stream);
       	 	stream.Position = 0;
       	 	
        	char[] ch = new char[stream.Length];
        	sr.Read(ch, 0, (int)stream.Length);
			stream.Close();
			
        	return Encoding.ASCII.GetBytes(ch);
    	}
    	
    	public object Deserialize(Type objType, byte[] inObj)
    	{    		
	    	MemoryStream memStream = new MemoryStream();
   	 		memStream.Write(inObj, 0, inObj.Length);
   	 		memStream.Seek(0, SeekOrigin.Begin);
   	 		
		    System.Xml.Serialization.XmlSerializer s = 
		        new System.Xml.Serialization.XmlSerializer(objType);
			return s.Deserialize(memStream);
    	}
    	
    	public void SendID()
    	{    		
    		List<byte[]> inputParts = new List<byte[]>();
    		inputParts.Add(cSendID);
    		inputParts.Add(Encoding.ASCII.GetBytes(Settings.myID));
    		    		        	
        	con.Send(formMessage(inputParts));
    	}
    	
    	public void GetID(byte[] inID)
    	{
				con.ID = Encoding.ASCII.GetString(inID);
    	}
    	
    	public void SendPeer()
    	{
    		List<byte[]> inputParts = new List<byte[]>();
    		inputParts.Add(cSendPeer);
        	inputParts.Add(Serialize(typeof(List<PeerEntry>), DataBase.peers ));
			
        	con.Send(formMessage(inputParts));
    	}
    	
    	public void GetPeer(byte[] inPeers)
    	{
    		DataBase.MergePeers((List<PeerEntry>)Deserialize(typeof(List<PeerEntry>), inPeers));
    	}
    	
    	public void SendTask()
    	{
    		List<byte[]> inputParts = new List<byte[]>();
    		inputParts.Add(cSendTask);
        	inputParts.Add(Serialize(typeof(List<TaskEntry>), DataBase.jobs ));
        	
        	con.Send(formMessage(inputParts));        	
    	}
    	
    	public void GetTask(byte[] inTasks)
    	{
    		DataBase.MergeTasks((List<TaskEntry>)Deserialize(typeof(List<TaskEntry>), inTasks));
    	}
    	
    	public void SendRoute()
    	{
    		List<byte[]> inputParts = new List<byte[]>();
    		inputParts.Add(cSendRoute);
    		
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
    		
    		inputParts.Add(Encoding.ASCII.GetBytes(sRoute));
    		
        	con.Send(formMessage(inputParts));
    	}
    	
    	public void GetRoute(byte[] inRoutes)
    	{
    		DataBase.MergeRoutes(Encoding.ASCII.GetString(inRoutes), con.ID);
    	}
    	
    	public void SendData(string inJobID, string inDataPath)
    	{
    		List<byte[]> inputParts = new List<byte[]>();
    		inputParts.Add(cSendData);
    		inputParts.Add(Encoding.ASCII.GetBytes(inJobID));
    		inputParts.Add(Encoding.ASCII.GetBytes(inDataPath));
			
			Stream dataFile = File.OpenRead(Settings.workingDirectory + inJobID + "\\" + inDataPath);
			byte[] data = new byte[(int)dataFile.Length];
			dataFile.Read(data, 0, data.Length);
			
        	inputParts.Add(data);
        	byte[] message  = formMessage(inputParts);
        	con.Send(message);
        	inputParts.Clear();
    	}
    	
    	public void GetData(byte[] inJobID, byte[] inDataPath, byte[] inDataFile)
    	{    		
			string jobID = Encoding.ASCII.GetString(inJobID);
			string dataPath = Encoding.ASCII.GetString(inDataPath);
			
			System.Diagnostics.Debug.WriteLine("added {0} = {1}", dataPath, Settings.myID);
			lock(DataBase.dataTrans)
			{
	   			DataBase.dataTrans[dataPath] = Settings.myID;
			}
			
			if(File.Exists(Settings.workingDirectory + jobID + "\\" + dataPath))
				return;
			if(!Directory.Exists(Settings.workingDirectory + jobID + "\\data"))
				Directory.CreateDirectory(Settings.workingDirectory + jobID + "\\data");
		
			Stream dataFile = File.OpenWrite(Settings.workingDirectory + jobID + "\\" + dataPath);
   	 		dataFile.Write(inDataFile, 0, inDataFile.Length);
			dataFile.Close();
    	}
    	
    	
    	
    	public void Parse(byte[] input, int size)
    	{
    		//Console.WriteLine(BitConverter.ToString(input, 0, size).Replace("-","") + " Size: " + size.ToString());
    		
    		#region ReceiveRes
			if (input[0] == cSendRes[0] && status == ConnectionStatus.Idle)
			{
				//status = ConnectionStatus.ReceivingRes;
				int inIndex = 1;
					
				int jobIDLen = BitConverter.ToInt32(input, inIndex);
								
				jobID = Encoding.ASCII.GetString(input, sizeof(Int32)+1, jobIDLen);
				inIndex += sizeof(Int32) + jobIDLen;
				int dataPathLen = BitConverter.ToInt32(input, inIndex);
				//Console.WriteLine("Packet size {0}, job Length {1}, Path Len {2}", size.ToString(), jobIDLen.ToString(), dataPathLen.ToString());
				dataPath = Encoding.ASCII.GetString(input, sizeof(Int32)+inIndex, dataPathLen);
						Console.WriteLine("Received {0}", dataPath);
				inIndex += sizeof(Int32) + dataPathLen;
        	
				int length = BitConverter.ToInt32(input, inIndex);
				inIndex += sizeof(Int32);
				ReceivedData = new byte[length];
				
				//Console.WriteLine("Getting {0} from job {1}", dataPath, jobID);
				
				
				if(length<1)
					return;
				if(size<length)
				{
				//	file = File.OpenWrite(Settings.workingDirectory + jobID + "\\" + dataPath);
					len = length;
					toGet = len - size + inIndex;
					Array.Copy(input, inIndex, ReceivedData, 0, size-inIndex);
       	 			//file.Write(input, inIndex, size-inIndex);
					index = size-inIndex;
					
					return;
				}
				Array.Copy(input, inIndex, ReceivedData, 0, length);
								
				if(!Directory.Exists(Settings.workingDirectory + jobID + "\\result"))
					Directory.CreateDirectory(Settings.workingDirectory + jobID + "\\result");
				
				
					string[] ress = dataPath.Split('\\');
					string conID;
					
					lock(DataBase.dataTrans)
					{
						conID = DataBase.dataTrans["data\\" + ress[ress.Length - 1]];
						DataBase.dataTrans.Remove("data\\" + ress[ress.Length - 1]);
					}
					
					
			    	Connection res = DataBase.cons.Find(
			    		delegate(Connection connect)
			    		{
			    			return connect.ID == conID;
			    		}
			    	);
					
					if(res != null)
						res.mp.dataReceived++;
				
				
       	 		file.Write(ReceivedData, 0, length);
       	 		//file.Write(input, inIndex, length);
				file.Close();
				
				//con.Send(cReceived);
					status = ConnectionStatus.Idle;
					return;
			}
    		#endregion
    		
    		#region ReceiveExec
			if (input[0] == cSendExec[0] && status == ConnectionStatus.Idle)
			{
				if(File.Exists(Settings.workingDirectory + askedFor + "\\task.exe"))
				{
					return;
				}
				
					status = ConnectionStatus.ReceivingExec;
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
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, ReceivedData, 0, length);
				
				if(!Directory.Exists(Settings.workingDirectory + askedFor))
					Directory.CreateDirectory(Settings.workingDirectory + askedFor);
				
				Stream exec = File.OpenWrite(Settings.workingDirectory + askedFor + "\\task.exe");
				
       	 		exec.Write(ReceivedData, 0, length);
				
				//con.Send(cReceived);
					status = ConnectionStatus.Idle;
				askedFor = "";
					return;
			}
			
			if(status == ConnectionStatus.ReceivingExec)
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
					
					//con.Send(cReceived);
					status = ConnectionStatus.Idle;
					askedFor = "";
				}
					return;
				
			}
    		#endregion
    		    		
    		
    		
    		
    		#region AskedForExec
			if (input[0] == cAskExec[0]  && status == ConnectionStatus.Idle)
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
					return;
			}
    		#endregion
    		
    		#region AskedForRes
			if (input[0] == cAskRes[0]  && status == ConnectionStatus.Idle)
			{
				int inIndex = 1;
					
				int jobIDLen = BitConverter.ToInt32(input, inIndex);
				jobID = Encoding.ASCII.GetString(input, sizeof(Int32)+inIndex, jobIDLen);
				inIndex += sizeof(Int32) + jobIDLen;
				
				int dataPathLen = BitConverter.ToInt32(input, inIndex);
				dataPath = Encoding.ASCII.GetString(input, sizeof(Int32)+inIndex, dataPathLen);
				
				if(!File.Exists(Settings.workingDirectory + jobID + "\\" + dataPath))
					return;
				
				try{
					SendRes(jobID, dataPath);
				}
				catch(Exception e)
				{
					System.Diagnostics.Debug.WriteLine(e.ToString());
				}
					return;
			}
    		#endregion
    		
			if (input[0] == cPing[0]  && status == ConnectionStatus.Idle)
			{
				Console.WriteLine("Ping");
				//con.Send(cReceived);
			}
			
			if(status == ConnectionStatus.Ping)
			{
				if (input[0] == cReceived[0])
				{
					Console.WriteLine("Pong");
					status = ConnectionStatus.Idle;
				}
			}
    	}
    	
    	public void ContinueRes(byte[] input, int size)
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
				
						Console.WriteLine("Received {0}", dataPath);
					string[] ress = dataPath.Split('\\');
					string conID = "";
					
					lock(DataBase.dataTrans)
					{
						if(DataBase.dataTrans.ContainsKey("data\\" + ress[ress.Length - 1]))
						{
							conID = DataBase.dataTrans["data\\" + ress[ress.Length - 1]];
							DataBase.dataTrans.Remove("data\\" + ress[ress.Length - 1]);
						}
					}
					
					
			    	Connection res = DataBase.cons.Find(
			    		delegate(Connection connect)
			    		{
			    			return connect.ID == conID;
			    		}
			    	);
					
					if(res != null)
						res.mp.dataReceived++;
					
					if(!Directory.Exists(Settings.workingDirectory + jobID + "\\result"))
						Directory.CreateDirectory(Settings.workingDirectory + jobID + "\\result");
				
					Stream exec = File.OpenWrite(Settings.workingDirectory + jobID + "\\" + dataPath);
					
	       	 		exec.Write(ReceivedData, 0, len);
					
					//con.Send(cReceived);
					exec.Close();
					
					
					//file.Close();
					status = ConnectionStatus.Idle;
				}
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
    	
    	public void AskRes(string jobID, string indataPath)
    	{
    		byte[] msg = cAskRes;
    		//if(String.IsNullOrEmpty(jobID))
    		//	return;
    		//Console.WriteLine("Asked data {0] from job {1}", indataPath, jobID);
						
        	byte[] data = Encoding.ASCII.GetBytes(jobID);
        	byte[] dataLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes(jobID).Length);
        	
        	
        	string[] ress = indataPath.Split('\\');
			
			byte[] dataPath = Encoding.ASCII.GetBytes("result\\" + ress[ress.Length - 1]);
        	byte[] dataPathLen = BitConverter.GetBytes(Encoding.ASCII.GetBytes("result\\" + ress[ress.Length - 1]).Length);
        	
        	byte[] message = new byte[msg.Length + data.Length + dataLen.Length + dataPath.Length + dataPathLen.Length];
        	int index = 0;
        	Array.Copy(msg, 0, message, index, msg.Count());
        	index += msg.Count();
        	Array.Copy(dataLen, 0, message, index, dataLen.Count());
        	index += dataLen.Count();
        	Array.Copy(data, 0, message, index, data.Count());
        	index += data.Count();
        	Array.Copy(dataPathLen, 0, message, index, dataPathLen.Count());
        	index += dataPathLen.Count();
        	Array.Copy(dataPath, 0, message, index, dataPath.Count());
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
    	
    	
    	public void SendRes(string injobID, string indataPath)
    	{
			Console.WriteLine("Sent {0} = {1}", indataPath, con.ID);
    		byte[] msg = cSendRes;
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
    			Thread.Sleep(Settings.Pause);
    			if (status == ConnectionStatus.Ping)
    			{
    				Console.WriteLine("Ping have not been answered");
    				con.Dispose();
    				continue;
    			}
    			if (status == ConnectionStatus.Idle)
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
    				/*
    				else if(!String.IsNullOrEmpty(askedFor) && !File.Exists(Settings.workingDirectory + askedFor + "\\task.exe"))
    				{
    					TaskManager.AskForExec(askedFor);
    				}
    				else
    				{
    					if(dataSent > dataReceived )
    					{
	    					lock(DataBase.dataTrans)
	    					{
					    		foreach(KeyValuePair<string, string> KV in DataBase.dataTrans)
					    		{
					    			if(String.Compare(con.ID, KV.Value) == 0)
					    			{
					    				AskRes("1", KV.Key);
    									break;
					    			}
					    		}
	    					}
    					}
    					
    					if(dataSent - dataReceived > dataWindow)
    						continue;
			    			//Console.WriteLine("New round");
			    			*/
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
						//status = ConnectionStatus.Ping;
    					//con.Send(cPing);
    				}
    				continue;
    			//}
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
    		DataBase.cons.Add(this);
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
            m_semaphore.WaitOne(1000);
					mp.getData(state.buffer, bytesRead);
            m_semaphore.Release();
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