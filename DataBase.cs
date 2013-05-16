using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace DGPGrid
{	
	public class TaskEntry
	{
		public String id;
		public String name;
		public String exec;
		public String workDir;
		public String master;
		public List<String> data = new List<String>();
		public List<String> result = new List<String>();
		public List<String> subscribed = new List<String>();
		public List<String> unsubscribed = new List<String>();
		
		public TaskEntry()
		{
		}
		
		public TaskEntry(String inid, String inname, String inexec, String inworkDir, List<String> indata, List<String> inresult, List<String> insubscribed, List<String> inunsubscribed, string inmaster)
		{
			id = inid;
			name = inname;
			exec = inexec;
			workDir = inworkDir;
			data = indata;
			result = inresult;
			subscribed = insubscribed;
			unsubscribed = inunsubscribed;
			master = inmaster;
		}
	}
	public class PeerEntry
	{
		public String id;
		public String ip;
		
		public PeerEntry()
		{
		}
		
		public PeerEntry(String inid, String inip)
		{
			id = inid;
			ip = inip;
		}
	}
	
    public static class DataBase
    {
    	// <IP:port, ConnectionHandle>
        public static Dictionary<String, Connection> conBase = new Dictionary<String, Connection>();
        // <IP:port, ID>
        //public static Dictionary<String, String> peerBase = new Dictionary<String, String>();
        
		public static List<TaskEntry> jobs = new List<TaskEntry>();
		public static List<PeerEntry> peers = new List<PeerEntry>();
        
        #region Connections
        public static void AddConn(String Address, Connection conn)
        {
        	if(!conBase.ContainsKey(Address))
    			conBase.Add(Address, conn);
        }
        
        public static void AddPeer(String iden, String Address)
        {
        	PeerEntry res = peers.Find(
        		delegate(PeerEntry peer)
        		{
        			return peer.id == iden;
        		}
        	);
            if (res == null)
            {
            	peers.Add(new PeerEntry(iden, Address));
    			setPeersChanged();
        	}
        }
        
        public static void MergePeers(List<PeerEntry> inpeers)
        {
        	foreach(PeerEntry peer in inpeers)
        	{
        		AddPeer(peer.id, peer.ip);
        	}
        }
        
        public static void MergeTasks(List<TaskEntry> injobs)
        {
        	foreach(TaskEntry job in injobs)
        	{
        		TaskManager.addTask(job.id, job.name, job.exec, job.workDir, job.data, job.result, job.subscribed, job.unsubscribed, job.master);
        	}
        }
        
        
        public static void DelConn(String Address)
        {
    		conBase.Remove(Address);
        }
        
        public static void ListConn()
        {
			foreach (KeyValuePair<String, Connection> pair in conBase)
			{
			    Console.WriteLine("{0}", pair.Key);
       		}
    	}
                
        public static String GetPeers()
        {
        	String res = "";
        	bool first = true;
        	foreach (PeerEntry peer in peers)
			{
				if(first)
				{
			    	res = peer.id + " " + peer.ip ;
			    	first = false;
				}
				else
			    	res += "\r\n" + peer.id + " " + peer.ip ;
       		}
			return res;
        }
        
        public static void ListPeer()
        {
        	Console.WriteLine(GetPeers());
    	}
        
        public static Connection getConn(string Address)
        {
        	Connection con = null;
        	conBase.TryGetValue(Address, out con);
        	return con;
        }
        
        public static void setPeersChanged()
        {
        	Connection con = null;
			foreach (KeyValuePair<String, Connection> pair in conBase)
			{
			    con = pair.Value;
			    con.setPeerChanged();
       		}
        }
        public static void setTasksChanged()
        {
        	Connection con = null;
			foreach (KeyValuePair<String, Connection> pair in conBase)
			{
			    con = pair.Value;
			    con.setTaskChanged();
       		}
        }
        public static void Save()
        {
		    System.Xml.Serialization.XmlSerializer sPeer = 
		        new System.Xml.Serialization.XmlSerializer(typeof(List<PeerEntry>));
			TextWriter PeerFile = new StreamWriter(Settings.workingDirectory + "peers.xml");
		    System.Xml.Serialization.XmlSerializer sTask = 
		        new System.Xml.Serialization.XmlSerializer(typeof(List<TaskEntry>));
			TextWriter TaskFile = new StreamWriter(Settings.workingDirectory + "tasks.xml");
			
			sPeer.Serialize(PeerFile, DataBase.peers);
			sTask.Serialize(TaskFile, DataBase.jobs);
        }
        
        public static void Restore()
        {
		    System.Xml.Serialization.XmlSerializer sPeer = 
		        new System.Xml.Serialization.XmlSerializer(typeof(List<PeerEntry>));
		    if(!File.Exists(Settings.workingDirectory + "peers.xml"))
		       return;
		    FileStream PeerFile = new FileStream(Settings.workingDirectory + "peers.xml", FileMode.Open, FileAccess.Read, FileShare.Read);
			
			List<PeerEntry> inpeers = (List<PeerEntry>)sPeer.Deserialize(PeerFile);
			PeerFile.Close();
			peers = inpeers;
			
		    System.Xml.Serialization.XmlSerializer sTask = 
		        new System.Xml.Serialization.XmlSerializer(typeof(List<TaskEntry>));
		    if(!File.Exists(Settings.workingDirectory + "tasks.xml"))
		       return;
		    FileStream TaskFile = new FileStream(Settings.workingDirectory + "tasks.xml", FileMode.Open, FileAccess.Read, FileShare.Read);
			
			List<TaskEntry> injobs = (List<TaskEntry>)sTask.Deserialize(TaskFile);
			TaskFile.Close();
			jobs = injobs;
        }
        #endregion
        
    }
}