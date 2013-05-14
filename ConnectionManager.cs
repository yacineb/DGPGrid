using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace DGPGrid
{
    public class Logger
    {
        public enum LogLevel
        {
            Fatal = 0,
            Error = 1,
            Debug = 2,
            Warning = 3,
            Info = 4
        }

        private LogLevel m_logLevel;
        private static Logger m_logger = new Logger();

        // Private API

        private Logger() { m_logLevel = LogLevel.Info; }

        private void Log (String message)
        {
        	System.Diagnostics.Debug.WriteLine(message);
        }
        
        private void PrivateFatal(String message)
        {
            if (m_logLevel >= LogLevel.Fatal)
            {
                Log ("FATAL: " + message);
            }
        }

        private void PrivateError(String message)
        {
            if (m_logLevel >= LogLevel.Error)
            {
                Log ("ERROR: " + message);
            }
        }

        private void PrivateDebug(String message)
        {
            if (m_logLevel >= LogLevel.Debug)
            {
                Log ("DEBUG: " + message);
            }
        }

        private void PrivateWarning(String message)
        {
            if (m_logLevel >= LogLevel.Warning)
            {
                Log ("WARNING: " + message);
            }
        }

        private void PrivateInfo(String message)
        {
            if (m_logLevel >= LogLevel.Info)
            {
                Log ("INFO: " + message);
            }
        }

        // Public API

        public static void Fatal(String message)
        {
            m_logger.PrivateFatal(message);
        }

        public static void Error(String message)
        {
            m_logger.PrivateError(message);
        }

        public static void Debug(String message)
        {
            m_logger.PrivateDebug(message);
        }

        public static void Warning(String message)
        {
            m_logger.PrivateWarning(message);
        }

        public static void Info(String message)
        {
            m_logger.PrivateInfo(message);
        }

    }
    
	public class ConnectionManager
	{
		// Thread signal.
		public static ManualResetEvent allDone = new ManualResetEvent(false);
		public static ManualResetEvent connectDone = new ManualResetEvent(false);
		
		public ConnectionManager()
		{
		}
		
		public static void StartListening(int port)
		{
			
            List<IPAddress> AddressesList = new List<IPAddress>();
            NetworkInterface[] adapters = NetworkInterface.GetAllNetworkInterfaces();

            foreach (NetworkInterface adapter in adapters)
            {
                IPInterfaceProperties properties = adapter.GetIPProperties();
                foreach (IPAddressInformation uniCast in properties.UnicastAddresses)
                {
                    if (!IPAddress.IsLoopback(uniCast.Address) && uniCast.Address.AddressFamily != AddressFamily.InterNetworkV6)
                    {
                        AddressesList.Add(uniCast.Address);
                    }
                }
            }
            
			// Temp storage for incoming data.
			byte[] recvDataBytes = new Byte[1024];
		
			// Make endpoint for the socket.
			//IPAddress serverAdd = IPAddress.Any;
			
			//IPEndPoint ep = new IPEndPoint(serverAdd, port);
			
			// Create a TCP/IP socket for listner.
			//Socket listenerSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
    		//DataBase.AddPeer(serverAdd+":"+port, "1");
			
    		
            foreach(IPAddress address in AddressesList)
            {
            	
				// Bind the socket to the endpoint and wait for listen for incoming connections.
				try
				{
	                Socket listenerSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
	                IPEndPoint ep = new IPEndPoint(address, port);
	                DataBase.AddPeer("i"+address.ToString()+":"+port, address.ToString()+":"+port);
	                
					listenerSock.Bind(ep);
					listenerSock.Listen(10);
					
					Task.Factory.StartNew(() => 
						{
							while (true)
							{
								// Set the event to nonsignaled state.
								allDone.Reset();
								
								// Start an asynchronous socket to listen for connections.
								//Console.WriteLine("Waiting for Client...");
								listenerSock.BeginAccept(new AsyncCallback(AcceptCallback),	listenerSock);
								
								// Wait until a connection is made before continuing.
								allDone.WaitOne();
							}
						}
					);
					
				}
				catch (Exception e)
				{
					Console.WriteLine(e.ToString());
				}		
            }	
		}
			
		public static void AcceptCallback(IAsyncResult ar)
		{
			// Signal the main thread to continue.
			allDone.Set();
			
			// Get the socket that handles the client request.
			Socket listener = (Socket)ar.AsyncState;
			Socket handler = listener.EndAccept(ar);
			
			// Create the state object.
			StateObject state = new StateObject();
			state.workSocket = handler;
			Task.Factory.StartNew(() => new Connection(handler, state));
		}
		
		public static void CreateConnection(String ipAddress, int port)
		{
			
            Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
				StateObject state = new StateObject();
				state.workSocket = socket;
            	socket.Connect(ipAddress, port);
    			DataBase.AddPeer("i"+ipAddress+":"+port, ipAddress+":"+port);
				Task.Factory.StartNew(() => new Connection(socket, state));
		}
	}
}