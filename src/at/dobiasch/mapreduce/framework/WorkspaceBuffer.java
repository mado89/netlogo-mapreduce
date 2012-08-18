package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;

import org.nlogo.agent.Observer;
import org.nlogo.api.CompilerException;
import org.nlogo.api.JobOwner;
import org.nlogo.api.SimpleJobOwner;
import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.CompilerResults;
import org.nlogo.nvm.Procedure;

public class WorkspaceBuffer
{
	public class Element
	{
		public HeadlessWorkspace ws;
		public Procedure map;
		public Procedure reduce;
		public JobOwner owner;
	}
	
	private Queue<Element> q;
	private Semaphore available;
	private int size;
	private String world;
	private String model;
	
	public WorkspaceBuffer(int size, String world, String model) throws IOException
	{
		available= new Semaphore(0);
		q= new LinkedList<Element>(); 
		
		this.size= size;
		this.world= world;
		this.model= model;
		
		this.createWorkspaces();
	}
	
	private void createWorkspaces() throws IOException
	{
		for(int i= 0; i < size + 1; i++)
		{
			HeadlessWorkspace ws= HeadlessWorkspace.newInstance();
			
			ws.open(model);
			
			StringReader sr = new StringReader(world);
			
			ws.importWorld(sr);
			
			Element e= new Element();
			e.ws= ws;
			e.owner= new SimpleJobOwner("MapRed", ws.world.mainRNG,Observer.class);
			q.offer(e);
			available.release();
		}
	}
	
	public void compileComands(String map, String reduce) throws CompilerException
	{
		for(Element e : q)
		{
			scala.Option<String> x = scala.Option.apply(null);
			CompilerResults res= e.ws.compiler().compileMoreCode(
				"to __evaluator [] __observercode " + map + " __key __value\n__done end", x, 
				e.ws.world.program(), e.ws.getProcedures(), e.ws.getExtensionManager());
			res.head().init(e.ws);
			e.map= res.head();
			
			res= e.ws.compiler().compileMoreCode(
				"to __evaluator [] __observercode " + reduce + " __key __value\n__done end", x, 
				e.ws.world.program(), e.ws.getProcedures(), e.ws.getExtensionManager());
			res.head().init(e.ws);
			e.reduce= res.head();
		}
	}
	
	/**
	 * Needed! Call this on shutdown. Disposes all workspaces
	 */
	public void dispose()
	{
		for(Element e : q)
		{
			try
			{
				e.ws.dispose();
			}
			catch (InterruptedException e1)
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	public Element get() throws InterruptedException
	{
		// logger.debug("get: " + q.size());
		available.acquire();
		
		return hget();
	}
	
	protected synchronized Element hget()
	{
		return q.poll();
	}
	
	public void release(Element workspace)
	{
		// logger.debug("release: " + q.size());
		hr(workspace);
		available.release();
	}
	
	protected synchronized void hr(Element workspace)
	{
		q.offer(workspace);
	}
}
