#define DEBUG
using System;
using System.IO;
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
		public static string myID = "";
		public static string appPath = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
		public static string workingDirectory = appPath + "\\";
		
        public static void Save()
        {
        	StreamWriter writer = new StreamWriter(appPath + "\\config.ini");
        	writer.WriteLine("WorkingDirectory=" + workingDirectory);
        	writer.WriteLine("ID=" + myID);
        	writer.Close();
        }
        
        public static void Restore()
        {
        	if(File.Exists(appPath + "\\config.ini"))
        	{
	        	StreamReader reader = new StreamReader(appPath + "\\config.ini");
				string line;
	        	while ((line = reader.ReadLine()) != null)
	        	{
				    string[] items = line.Split('=');
				    if(string.Compare(items[0], "WorkingDirectory") == 0)
				       workingDirectory=items[1];
				    if(string.Compare(items[0], "ID") == 0)
				       myID=items[1];
				}
	        	reader.Close();
        	}
        	if(String.IsNullOrEmpty(myID))
        	{
        		Console.WriteLine("Enter your ID");
        		myID = Console.ReadLine();
        	}
        }
	}
	
	public class DGPGrid
	{			
		public static int Main(String[] args)
		{
			Settings.Restore();
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
			Settings.Save();
			DataBase.Save();
			return 0;
		}
	}
} 