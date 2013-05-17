using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace DGPGrid
{	
	
	public static class TaskManager
	{
		
		public static int timestamp()
		{
			return (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
		}
		
		public static int poolVol = 10;
		public static List<string> queue = new List<string>();
        public static List<string> pool = new List<string>();
        //public static Dictionary<string, string> dataTrans = new Dictionary<string, string>();
        private  static System.Threading.Semaphore m_semaphore = new System.Threading.Semaphore(1, 1);
		
		public static void addTask(String id, String name )
		{
			TaskEntry job = new TaskEntry();
			job.id = id;
			job.master = Settings.myID;
			job.name = name;
			job.workDir = Settings.workingDirectory + id + "\\";
			if (File.Exists(job.workDir+"task.exe"))
				job.exec = "task.exe";
			lock(DataBase.dataTrans)
			{
			foreach (String file in Directory.GetFiles(job.workDir + "data\\"))
			{
				String[] files = file.Split('\\');
				if (File.Exists(job.workDir + "data\\" + files[files.Length - 1]))
				{
					job.data.Add("data\\" + files[files.Length - 1]);
					DataBase.dataTrans.Add("data\\" + files[files.Length - 1], Settings.myID);
				}
			}
			}
			if(job.master == Settings.myID)
			{
				string[] IDs =new  string[1]{Settings.myID};
				Route r = new Route(0, IDs);
				job.routes.Add(r);
			}
			DataBase.jobs.Add(job);
			DataBase.setTasksChanged();
            if(!queue.Contains(id))
            	Subscribe(id);
			if(!Directory.Exists(Settings.workingDirectory + id + "\\data"))
				Directory.CreateDirectory(Settings.workingDirectory + id + "\\data");
			if(!Directory.Exists(Settings.workingDirectory + id + "\\result"))
				Directory.CreateDirectory(Settings.workingDirectory + id + "\\result");
		}
		
		public static void addTask(String id, String name, String exec, String data, String workingDir )
		{
			TaskEntry job = new TaskEntry();
			job.id = id;
			job.name = name;
			job.master = Settings.myID;
			job.workDir = workingDir;
			String[] names = exec.Split('\\');
			//System.Diagnostics.Debug.WriteLine(workingDir+names[names.Length - 1]);
			if (!File.Exists(workingDir+names[names.Length - 1]))
				File.Copy(exec, workingDir+names[names.Length - 1]);
			job.exec = names[names.Length - 1];
			foreach (String file in Directory.GetFiles(data))
			{
				String[] files = file.Split('\\');
				//System.Diagnostics.Debug.WriteLine(workingDir + "data\\" + files[files.Length - 1]);
				if (!File.Exists(workingDir + "data\\" + files[files.Length - 1]))
					File.Copy(file, workingDir + "data\\" + files[files.Length - 1]);
				job.data.Add("data\\" + files[files.Length - 1]);
			}
			if(job.master == Settings.myID)
			{
				string[] IDs =new  string[1]{Settings.myID};
				Route r = new Route(0, IDs);
				job.routes.Add(r);
			}
			DataBase.jobs.Add(job);
			DataBase.setTasksChanged();
            if(!queue.Contains(id))
            	Subscribe(id);            
			if(!Directory.Exists(Settings.workingDirectory + id + "\\data"))
				Directory.CreateDirectory(Settings.workingDirectory + id + "\\data");
			if(!Directory.Exists(Settings.workingDirectory + id + "\\result"))
				Directory.CreateDirectory(Settings.workingDirectory + id + "\\result");
		}
		
		public static void addTask(String iden, String name, String exec, String workDir, List<String> data, List<String> inresult, List<String> insubscribed, List<String> inunsubscribed, string inmaster )
		{
			
        	TaskEntry res = DataBase.jobs.Find(
        		delegate(TaskEntry job)
        		{
        			return job.id == iden;
        		}
        	);
            if (res == null)
            {
            	DataBase.jobs.Add(new TaskEntry(iden, name, exec, Settings.workingDirectory + iden, data, inresult, insubscribed, inunsubscribed, inmaster));
    			DataBase.setTasksChanged();
        	}
            else
            {
            	foreach(String unsub in inunsubscribed)
            	{
            		if(!res.unsubscribed.Contains(unsub))
            			res.unsubscribed.Add(unsub);
            		if(res.subscribed.Contains(unsub))
            			res.subscribed.Remove(unsub);
            	}
            	foreach(String sub in insubscribed)
            	{
            		if(!res.subscribed.Contains(sub))
            			res.subscribed.Add(sub);
            		if(res.unsubscribed.Contains(sub))
            			res.unsubscribed.Remove(sub);
            	}
            	foreach(String resul in inresult)
            	{
            		if(!res.result.Contains(resul))
            			res.result.Add(resul);
            	}
            }
            if(!queue.Contains(iden))
            	Subscribe(iden);
			if(!Directory.Exists(Settings.workingDirectory + iden + "\\data"))
				Directory.CreateDirectory(Settings.workingDirectory + iden + "\\data");
			if(!Directory.Exists(Settings.workingDirectory + iden + "\\result"))
				Directory.CreateDirectory(Settings.workingDirectory + iden + "\\result");
		}
		
		public static void ListTasks()
		{
			foreach(TaskEntry job in DataBase.jobs)
			{
				Console.WriteLine("ID: {0}; Name: {1}; Master: {2}; Subscribed: {3}; Results/Data: {4}/{5}",
				                  job.id, job.name, job.master, job.subscribed.Count(), job.result.Count(), job.data.Count());
				foreach(Route KV in job.routes)
				{
					string sCons = "";
					foreach(string con in KV.cons)
						sCons += con + ", ";
					Console.WriteLine("Metric: {0}; Connections: {1}", KV.metric, sCons);
				}
						
			}
		}
		
		public static void Subscribe(string iden)
		{
        	TaskEntry job = DataBase.jobs.Find(
        		delegate(TaskEntry peer)
        		{
        			return peer.id == iden;
        		}
        	);
            if (job != null)
            {
				queue.Add(iden);
				job.subscribed.Add(Settings.myID);
				DataBase.setTasksChanged();
        	}
            if(!File.Exists(Settings.workingDirectory + iden + "\\task.exe"))
               AskForExec(iden);
		}
		
        public static void AskForExec(string inID)
        {
        	TaskEntry job = DataBase.jobs.Find(
        		delegate(TaskEntry peer)
        		{
        			return peer.id == inID;
        		}
        	);
        	
			Task.Factory.StartNew(() => 
				{
                  	while(!File.Exists(Settings.workingDirectory + inID + "\\task.exe"))
                  	{
			        	foreach(string peerID in job.subscribed)
			        	{
			        		foreach(KeyValuePair<string, Connection> KV in DataBase.conBase)
			        		{
			        			if(KV.Value.ID == peerID)
			        			{
			        				KV.Value.AskForExec(inID);
			        			}
			        		}
			        	}
						Thread.Sleep(1000);
                  	}
             	}
             );
        }
		
		public static void RetainPool( TaskEntry job )
		{
			Random r_num = new Random();
			List<string> localData = new List<string>();
			string[] ress;
			string result;
			int index;
			lock(DataBase.dataTrans)
			{
				foreach(KeyValuePair<string, string> KV in DataBase.dataTrans)
				{
					//System.Diagnostics.Debug.WriteLine("read {0} = {1}", KV.Key, KV.Value);
			 		ress = KV.Key.Split('\\');
					result = "result\\" + ress[ress.Length - 1];
					if(KV.Value == Settings.myID && !job.result.Contains(result))
						localData.Add(KV.Key);
				}
			
				poolVol = (10 < localData.Count()) ? 10 : localData.Count();
				
				
				while(pool.Count() != poolVol)
				{
					index = r_num.Next(0,localData.Count());
					pool.Add(localData[index]);
					localData.RemoveAt(index);
				}
			}
		}
			
		public static void StartWorker()
		{
			Task.Factory.StartNew(() => 
				{
			        int start = 0, time = 0, reps;
			        bool started = false;
			        List<string> toRemove = new List<string>();
			    	string filename, result;
			    	string[] ress;
        			
                  	while (true)
                  	{
						if(!started)
							start = timestamp();
				  		if(queue.Count() == 0)
				  		{
							Thread.Sleep(1000);
				  			continue;
				  		}
				  		else
				  		{
				  			time = timestamp() - start;
				  			started = true;
				  		}
				  		
				    	TaskEntry job = DataBase.jobs.Find(
				    		delegate(TaskEntry peer)
				    		{
				    			return peer.id == queue[0];
				    		}
				    	);
				  		
			        	if(job.result.Count() >= job.data.Count())
			        	{
							//System.Diagnostics.Debug.WriteLine("Task finished");
							Console.WriteLine("Finished in {0} seconds", time.ToString());
							started = false;
							queue.RemoveAt(0);
			        	}
				  		
				  		RetainPool(job);
				  		
	 					if(pool.Count() == 0)
	 						continue;
	 					
	 					reps = 0;
			        	
			        	foreach(string dataset in pool)
			        	{
			        		System.Diagnostics.Process p = new System.Diagnostics.Process();
							ress = dataset.Split('\\');
							filename = Settings.workingDirectory + job.id + "\\data\\" + ress[ress.Length - 1];
							result = Settings.workingDirectory + job.id + "\\result\\" + ress[ress.Length - 1];
							
							System.Diagnostics.Debug.WriteLine("data: {0}\nres: {1}", filename,result);
							p.StartInfo.Arguments = filename + " " + result;
							p.StartInfo.FileName = Settings.workingDirectory + job.id + "\\" + job.exec;
							p.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
							p.Start();
				 			p.WaitForExit();
				 			if(p.ExitCode == 0)
				 			{
				 				if(!job.result.Contains("result\\" + ress[ress.Length - 1]))
				 					job.result.Add("result\\" + ress[ress.Length - 1]);
								toRemove.Add(dataset);
				 			}
				 			reps++;
				 				if(reps > 5)
				 			{
				 				break;
				 			}
			        	}
			        	lock(DataBase.dataTrans)
			        	{
				        	foreach(string dataset in toRemove)
				        	{
				        		pool.Remove(dataset);
	            				DataBase.dataTrans.Remove(dataset);
				        		
				        	}
			        	}
			        	toRemove.Clear();
						DataBase.setTasksChanged();
			        	
                  	}
                }
            );
		}
	}
}