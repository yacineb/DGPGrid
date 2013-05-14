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
		public const int BufferSize = 4096;
		// Receive buffer.
		public byte[] buffer = new byte[BufferSize];
		// Received data string.
		//public StringBuilder sb = new StringBuilder();
		public bool received = false;
	}
    
    public class MessageProcessor
    {
        private enum ConnectionState
        {
            Idle = 0,
            Receiving,
            ReceivingPeers,
            ReceivingTasks,
            Sending,
            Ping
        };
        
    	public Connection con = null;
        private ConnectionState state;
        public bool peerChanged = true;
        public bool taskChanged = true;
        
		#region Commands
		byte[] cComplete = { 0 };
		byte[] cReceived = { 1 };
		byte[] cPing = { 2 };
		byte[] cSendPeer = { 3 };
		byte[] cSendTask = { 4 };
		#endregion
		byte[] peerRec = null;
		int index = 0;
		int toGet = 0;
		int len = 0;
			
    	public MessageProcessor(Connection connect)
    	{
    		state = ConnectionState.Idle;
    		con = connect;
    	}
    	
    	public void Parse(byte[] input, int size)
    	{
    		//Console.WriteLine(BitConverter.ToString(input, 0, size).Replace("-","") + " Size: " + size.ToString());
    		
    		#region ReceivePeer
			if (input[0] == cSendPeer[0])
			{
				int length = BitConverter.ToInt32(input, 1);
				peerRec = new byte[length];
				if(length<1)
					return;
				if(size<length)
				{
					len = length;
					toGet = len - size + sizeof(Int32) + 1;
					Array.Copy(input, sizeof(Int32)+1, peerRec, 0, size-1-sizeof(Int32));
					index = size-1-sizeof(Int32);
					state = ConnectionState.ReceivingPeers;
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, peerRec, 0, length);
		    	MemoryStream memStream = new MemoryStream();
       	 		memStream.Write(peerRec, 0, length);
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
					Array.Copy(input, 0, peerRec, index, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, peerRec, index, toGet);
					index += toGet;
					toGet -= toGet;
				}
				if(toGet == 0)
				{
			    	MemoryStream memStream = new MemoryStream();
	       	 		memStream.Write(peerRec, 0, len);
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
			if (input[0] == cSendTask[0])
			{
				int length = BitConverter.ToInt32(input, 1);
				peerRec = new byte[length];
				if(length<1)
					return;
				if(size<length)
				{
					len = length;
					toGet = len - size + sizeof(Int32) + 1;
					Array.Copy(input, sizeof(Int32)+1, peerRec, 0, size-1-sizeof(Int32));
					index = size-1-sizeof(Int32);
					state = ConnectionState.ReceivingTasks;
					
					return;
				}
				Array.Copy(input, sizeof(Int32)+1, peerRec, 0, length);
		    	MemoryStream memStream = new MemoryStream();
       	 		memStream.Write(peerRec, 0, length);
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
					Array.Copy(input, 0, peerRec, index, size);
					toGet -= size;
					index += size;
				}
				else
				{
					Array.Copy(input, 0, peerRec, index, toGet);
					index += toGet;
					toGet -= toGet;
				}
				if(toGet == 0)
				{
			    	MemoryStream memStream = new MemoryStream();
	       	 		memStream.Write(peerRec, 0, len);
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
    		
			if (input[0] == cPing[0])
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
    				else
    				{
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
					try{
						mp.Parse(state.buffer, bytesRead);
					}
					catch (Exception e)
					{
						Console.WriteLine(e.ToString());
					}
					handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0, new AsyncCallback(ReadCallback), state);
				/*	// There might be more data, so store the data received so far.
					//state.sb.Append(Encoding.ASCII.GetString(state.buffer, 0, bytesRead));
					
					// Check for end-of-file tag. If it is not there, read
					// more data.
					content = Encoding.ASCII.GetString(state.buffer, 0, bytesRead);
					//content = BitConverter.ToString(state.buffer, 0, bytesRead);
					
					if (content.IndexOf("") > -1)
					{
						// All the data has been read from the
						// client. Display it on the console.
						//Console.WriteLine("Data : {1} \n Read {0} bytes from socket.", content.Length, content);
						
						mp.Parse(content);
						//state.received = true;
						// Echo the data back to the client.
						//Send(handler, Encoding.ASCII.GetBytes(content));
						
						handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0, new AsyncCallback(ReadCallback), state);
					}
					else
					{
						// Not all data received. Get more.
						handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0, new AsyncCallback(ReadCallback), state);
					}*/
				}
			}
			catch(SocketException e)
			{
				if(String.Equals(e.SocketErrorCode.ToString(), "ConnectionReset"))
				{
	            	m_semaphore.WaitOne();
    				Console.WriteLine("Connection with {0} lost", handle.RemoteEndPoint.ToString());
					DataBase.DelConn(handler.RemoteEndPoint.ToString());
					//DataBase.DelPeer(handler.RemoteEndPoint.ToString());	
	                m_semaphore.Release();			
				}
				else
					throw(e);
			}
		}
		
		public void Send(byte[] data)
		{
			// Convert the string data to byte data using ASCII encoding.
			//byte[] byteData = Encoding.ASCII.GetBytes(data);
			
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
				
				//handler.Shutdown(SocketShutdown.Both);
				//handler.Close();
			
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
    }    
}