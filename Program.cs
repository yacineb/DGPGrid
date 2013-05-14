#define DEBUG
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DGPGrid
{	
	public static class Settings
	{
		public static string workingDirectory = @"c:\grid\";
		public static string myID = "i46.242.49.21:3000";
	}
	
	public class DGPGrid
	{			
		public static int Main(String[] args)
		{
			DataBase.Restore();
			ConnectionManager.StartListening(3000);
			TaskManager.StartWorker();
			Console.WriteLine("Started");
			while (true)
			{
				String command = Console.ReadLine();
				if(CommandParser.Parse(command) == 1)
					break;
			}
			DataBase.Save();
			return 0;
		}
	}
} 