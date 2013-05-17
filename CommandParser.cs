using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace DGPGrid
{
	public static class CommandParser
	{
		public static int Parse(String str)
		{
			String[] param = str.Split(' ');
			String command = param[0].ToLower();
			
			#region Commands
			// Connect
			if(String.Equals(command, "connect"))
			{
				int port;
				
				if(param.Length < 3)
				{
					return 0;
				}
				
				try 
				{
					port = Int32.Parse(param[2]);
				}
				catch (FormatException)
				{
						Console.WriteLine("Port {0} is not valid input", param[2]);
						return 0;
				}
				Task.Factory.StartNew(() =>
					{
						try
						{
							ConnectionManager.CreateConnection(param[1], port);
						}
						catch (SocketException e)
						{
							if(String.Equals(e.SocketErrorCode.ToString(), "TimedOut"))
								Console.WriteLine("Connection to {0} timed out", param[1]);
							else if(String.Equals(e.SocketErrorCode.ToString(), "NetworkUnreachable"))
								Console.WriteLine("{0} is unreachable", param[1]);
							else
								throw(e);
						}
					}
				);
			
			}
			
			
			if(String.Equals(command, "send"))
			{
				String mes = "";
				if(param.Length < 3)
				{
					return 0;
				}
				
				for(int i = 2; i < param.Length; i++)
					mes += param[i];
				Connection con = DataBase.getConn(param[1]);
            	con.Send(System.Text.Encoding.Unicode.GetBytes(mes));
			}
			
			if(String.Equals(command, "data"))
			{
				foreach(KeyValuePair<string, string> KV in DataBase.dataTrans)
					Console.WriteLine("{0} = {1}", KV.Key, KV.Value);
			}
			
			// Show
			if(String.Equals(command, "show"))
			{
				if(param.Length < 2)
				{
					return 0;
				}
				if(String.Equals(param[1].ToLower(), "peers"))
				{
					DataBase.ListPeer();
				}
				if(String.Equals(param[1].ToLower(), "conns"))
				{
					DataBase.ListConn();
				}
				if(String.Equals(param[1].ToLower(), "tasks"))
				{
					TaskManager.ListTasks();
				}
			}
			// Load
			if(String.Equals(command, "load"))
			{
				if(param.Length < 3)
				{
					return 0;
				}
				TaskManager.addTask(param[1], param[2] );
			}
			
			// Subscribe
			if(String.Equals(command, "subscribe"))
			{
				if(param.Length < 2)
				{
					return 0;
				}
				TaskManager.Subscribe(param[1] );
			}
			// Exit
			if(String.Equals(command, "exit"))
			{
			   return 1;
			}
			#endregion
			
			return 0;
		}
	}
}

			                      	