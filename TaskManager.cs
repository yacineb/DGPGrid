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
		
		public static void addTask(String iden, String name, String exec, String workDir, List<String> data, List<String> inresult, List<String> insubscribed, List<String> inunsubscribed )
		{
			
        	TaskEntry res = DataBase.jobs.Find(
        		delegate(TaskEntry job)
        		{
        			return job.id == iden;
        		}
        	);
            if (res == null)
            {
            	DataBase.jobs.Add(new TaskEntry(iden, name, exec, Settings.workingDirectory + iden, data, inresult, insubscribed, inunsubscribed));
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
				Console.WriteLine("ID: {0}; Name: {1}; Exec: {2}; Subscribed: {3}; Results/Data: {4}/{5}",
				                  job.id, job.name, job.exec, job.subscribed.Count(), job.result.Count(), job.data.Count());
						
			}
		}
		
		public static void Subscribe(string iden)
		{
			if(DataBase.jobs.FindIndex(f => f.id == iden) >= 0)
				queue.Add(iden);
			   
				
		}
		public static void StartWorker()
		{
			Task.Factory.StartNew(() => 
				{
			        int queueLen = 0;
			        int pool = 10;
			        int reps;
			    	string filename, result;
			    	string[] ress;
        			Random r_num = new Random(); 
								System.Diagnostics.Debug.WriteLine("Worker started");
                  	while (true)
                  	{
    					Thread.Sleep(1000);
                  		reps = 0;
                  		if(queue.Count() == 0)
                  		{
							System.Diagnostics.Debug.WriteLine("Nothing to compute");
                  			continue;
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
			            	queue.Remove(queue[0]);
			            	continue;
			        	}
                  		while(pool > queueLen)
                  		{
								System.Diagnostics.Debug.WriteLine("trying to find work");
							do
							{
								filename = job.data[r_num.Next(0,job.data.Count())];
								ress = filename.Split('\\');
								result = "result\\" + ress[ress.Length - 1];
								if(!File.Exists(Settings.workingDirectory + job.id + "\\" + filename))
									continue;
								System.Diagnostics.Debug.WriteLine("trying to find data set");
					 			reps++;
				 				if(reps > 5)
				 				{
				 					break;
				 				}
							}       	
							while(job.result.Contains(result) || dataTrans.ContainsKey("data\\"+ress[ress.Length - 1]));
			 				if(reps > 5)
			 				{
			 					reps = 0;
			 					break;
			 				}
							dataTrans.Add("data\\"+ress[ress.Length - 1], Settings.myID);
							queueLen++;
                  		}
								System.Diagnostics.Debug.WriteLine("before starting worker");
			        	System.Diagnostics.Process p = new System.Diagnostics.Process();
			        	foreach(KeyValuePair<string, string> KV in dataTrans)
			        	{
								System.Diagnostics.Debug.WriteLine("Never found work");
			        		if(KV.Value != Settings.myID)
			        			continue;
							System.Diagnostics.Debug.WriteLine("No I did it");
							ress = KV.Key.Split('\\');
							filename = Settings.workingDirectory + job.id + "\\data\\" + ress[ress.Length - 1];
							result = Settings.workingDirectory + job.id + "\\result\\" + ress[ress.Length - 1];
							
							System.Diagnostics.Debug.WriteLine("data: {0}\nres: {1}", filename,result);
							p.StartInfo.Arguments = filename + " " + result;
							p.StartInfo.FileName = Settings.workingDirectory + job.id + "\\" + job.exec;
							p.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
							p.Start();
							System.Diagnostics.Debug.WriteLine("started");
				 			p.WaitForExit();
				 			if(p.ExitCode == 0)
				 			{
				 				job.result.Add("result\\" + ress[ress.Length - 1]);
				 				queueLen--;
				 			}
				 			reps++;
			 				if(reps > 5)
			 				{
			 					reps = 0;
			 					break;
			 				}
			        	}
                  	}
                }
            );
			/*
        	Random r_num = new Random(); 
        	String filename, result;
        	String[] ress;
        	
			do
			{
				filename = queue[0].data[r_num.Next(0,queue[0].data.Count())];
				ress = filename.Split('\\');
				result = queue[0].workDir + "result\\" + ress[ress.Length - 1];
			}       	
			while(queue[0].result.Contains(result));        	
        	
        	System.Diagnostics.Process p = new System.Diagnostics.Process();
			p.StartInfo.Arguments = filename + " " + result;
			p.StartInfo.FileName = queue[0].exec;
			p.StartInfo.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
			p.Start();
 			p.WaitForExit();
 			if(p.ExitCode == 0)
 				queue[0].result.Add(result);*/
		}
	}
}