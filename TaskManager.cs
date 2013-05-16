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
		
		public static List<string> queue = new List<string>();
        public static Dictionary<string, string> dataTrans = new Dictionary<string, string>();
        //public static Dictionary<string, string> dataTrans = new Dictionary<string, string>();
		
		public static void addTask(String id, String name )
		{
			TaskEntry job = new TaskEntry();
			job.id = id;
			job.master = Settings.myID;
			job.name = name;
			job.workDir = Settings.workingDirectory + id + "\\";
			if (File.Exists(job.workDir+"task.exe"))
				job.exec = "task.exe";
			foreach (String file in Directory.GetFiles(job.workDir + "data\\"))
			{
				String[] files = file.Split('\\');
				if (File.Exists(job.workDir + "data\\" + files[files.Length - 1]))
					job.data.Add("data\\" + files[files.Length - 1]);
			}
			DataBase.jobs.Add(job);
		}
		
		public static void addTask(String id, String name, String exec, String data, String workingDir )
		{
			TaskEntry job = new TaskEntry();
			job.id = id;
			job.name = name;
			job.master = Settings.myID;
			job.workDir = workingDir;
			String[] names = exec.Split('\\');
			System.Diagnostics.Debug.WriteLine(workingDir+names[names.Length - 1]);
			if (!File.Exists(workingDir+names[names.Length - 1]))
				File.Copy(exec, workingDir+names[names.Length - 1]);
			job.exec = names[names.Length - 1];
			foreach (String file in Directory.GetFiles(data))
			{
				String[] files = file.Split('\\');
				System.Diagnostics.Debug.WriteLine(workingDir + "data\\" + files[files.Length - 1]);
				if (!File.Exists(workingDir + "data\\" + files[files.Length - 1]))
					File.Copy(file, workingDir + "data\\" + files[files.Length - 1]);
				job.data.Add("data\\" + files[files.Length - 1]);
			}
			DataBase.jobs.Add(job);
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
		}
		
		public static void ListTasks()
		{
			foreach(TaskEntry job in DataBase.jobs)
			{
				Console.WriteLine("ID: {0}; Name: {1}; Master: {2}; Subscribed: {3}; Results/Data: {4}/{5}",
				                  job.id, job.name, job.master, job.subscribed.Count(), job.result.Count(), job.data.Count());
						
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
        	}				
		}
		
		public static void StartWorker()
		{
			Task.Factory.StartNew(() => 
				{
			        int queueLen = 0;
			        int pool = 10;
			        int start = 0, time = 0;
			        int reps, index;
			        bool found = false, started = false;
			        string tempid = "";
			        List<string> tempdata = new List<string>();
        			Dictionary<string, string> dataTemp = new Dictionary<string, string>();
			    	string filename, result;
			    	string[] ress;
        			Random r_num = new Random(); 
                  	while (true)
                  	{
                  		reps = 0;
                  		if(queue.Count() == 0)
                  		{
    						Thread.Sleep(1000);
    						if(started)
    						{
    							Console.WriteLine("Finished in {0} seconds", time.ToString());
    							started = false;
    						}
    						else
    							start = timestamp();
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
			            if (job == null)
			            {
							System.Diagnostics.Debug.WriteLine("Task not Found");
							queue.RemoveAt(0);
			            	continue;
			        	}
			            if(String.Compare(job.id, tempid) != 0)
			            {
			            	tempid = job.id;
			            	tempdata.Clear();
			            	tempdata = job.data.OfType<string>().ToList();
			            }
			            while(pool > queueLen && tempdata.Count() > 0)
                  		{
							do
							{
								index = r_num.Next(0,tempdata.Count());
								filename = tempdata[index];
								ress = filename.Split('\\');
								result = "result\\" + ress[ress.Length - 1];
								if(!File.Exists(Settings.workingDirectory + job.id + "\\" + filename))
									continue;
					 			reps++;
					 			found = job.result.Contains(result) || dataTrans.ContainsKey("data\\"+ress[ress.Length - 1]);
					 			if(found)
					 				tempdata.RemoveAt(index);
							}       	
							while((reps < tempdata.Count() && found) || (tempdata.Count() > 0 && found));
							if(!found)
							{
								dataTrans.Add("data\\"+ress[ress.Length - 1], Settings.myID);
				 				tempdata.RemoveAt(index);
								queueLen++;
							}
                  		}
			        	System.Diagnostics.Process p = new System.Diagnostics.Process();
	 					reps = 0;
			        	foreach(KeyValuePair<string, string> KV in dataTrans)
			        	{
			        		if(KV.Value != Settings.myID)
			        			continue;
							ress = KV.Key.Split('\\');
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
				 				job.result.Add("result\\" + ress[ress.Length - 1]);
								dataTemp.Add(KV.Key, KV.Value);
				 				queueLen--;
				 			}
				 			reps++;
				 				if(reps > 5)
				 			{
				 				break;
				 			}
			        	}
			        	foreach(KeyValuePair<string, string> KV in dataTemp)
			        	{
			        		dataTrans.Remove(KV.Key);
			        	}
			        	dataTemp.Clear();
			        	if(job.result.Count() == job.data.Count())
			        	{
							System.Diagnostics.Debug.WriteLine("Task finished");
							queue.RemoveAt(0);
			        	}
                  	}
                }
            );
		}
	}
}