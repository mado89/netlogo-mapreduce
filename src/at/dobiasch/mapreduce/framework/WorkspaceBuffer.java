package at.dobiasch.mapreduce.framework;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.nlogo.agent.Observer;
import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
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
		// ExecutorService pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		ExecutorService pool= Executors.newCachedThreadPool();
		CompletionService<Object> complet= new ExecutorCompletionService<Object>(pool);
		for(int i= 0; i < size; i++)
		{
			complet.submit(new Callable<Object>()
					{
						public Object call() throws Exception
						{
							HeadlessWorkspace ws= HeadlessWorkspace.newInstance();
							
							ws.open(model);
							
							StringReader sr = new StringReader(world);
							
							ws.importWorld(sr);
							
							Element e= new Element();
							e.ws= ws;
							e.owner= new SimpleJobOwner("MapReduce", ws.world.mainRNG,Observer.class);
							q.offer(e);
							available.release();
							System.out.println("WS opened");
							return null;
						}
				
					}
			);
		}
		try
		{
			pool.shutdown();
			for(int l= 0; l < size; l++)
				complet.take();
		}catch(InterruptedException e)
		{
			// throw new ExtensionException( e );
		}
	}
	
	/**
	 * If a parameter is null, the command is not compiled
	 * @param map
	 * @param reduce
	 * @throws CompilerException
	 */
	public void compileComands(String map, String reduce) throws CompilerException
	{
		if( map != null && reduce != null )
			this.compileCommandsAll(map, reduce);
		else if( map != null && reduce == null )
			this.compileCommandsMap(map);
		else if( map == null && reduce != null )
			this.compileCommandsReduce(reduce);
		else
			throw new CompilerException("Wrong call to MR-compilecommands", 0, 0, "Wrong call to MR-compilecommands");
	}
	
	private void compileCommandsAll(String map, String reduce) throws CompilerException
	{
		int i= 0;
		// for(Element e : q)
		for(i= 0; i < q.size(); i++)
		{
			Element e= q.poll();
			e.map= e.ws.compileCommands(map);
			/*scala.Option<String> x = scala.Option.apply(null);
			CompilerResults res= e.ws.compiler().compileMoreCode(
				"to __evaluator [] __observercode " + map + " __key __value\n__done end", x, 
				e.ws.world.program(), e.ws.getProcedures(), e.ws.getExtensionManager());
			res.head().init(e.ws);
			e.map= res.head(); */
			
			/*res= e.ws.compiler().compileMoreCode(
				"to __evaluator [] __observercode " + reduce + " __key __value\n__done end", x, 
				e.ws.world.program(), e.ws.getProcedures(), e.ws.getExtensionManager());
			res.head().init(e.ws);
			e.reduce= res.head();*/
			e.reduce= e.ws.compileCommands(reduce);
			System.out.println((i+1) + " Workspaces compiled");
			q.add(e);
		}
	}
	
	private void compileCommandsMap(String map) throws CompilerException
	{
		int i= 0;
		// for(Element e : q)
		for(i= 0; i < q.size(); i++)
		{
			Element e= q.poll();
			e.map= e.ws.compileCommands(map);
			
			q.add(e);
		}
	}
	
	private void compileCommandsReduce(String reduce) throws CompilerException
	{
		int i= 0;
		// for(Element e : q)
		for(i= 0; i < q.size(); i++)
		{
			Element e= q.poll();
			e.reduce= e.ws.compileCommands(reduce);
			
			q.add(e);
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

	public void resize(int size)
	{
		// TODO Auto-generated method stub
		
	}
}
