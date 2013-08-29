package org.nlogo.extensions.mapreduce.commands;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoList;
import org.nlogo.api.Syntax;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.MapReduceRun;
import at.dobiasch.mapreduce.MultiNodeRun;
import at.dobiasch.mapreduce.SingleNodeRun;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.FrameworkFactory;

/**
 * 
 * @author Martin Dobiasch
 */
public class MapReduce extends DefaultReporter
{
	public Syntax getSyntax()
	{
		return Syntax.reporterSyntax(new int[] {
				Syntax.StringType() /*Mapper*/, 
				Syntax.StringType() /*Reducer*/,
				Syntax.WildcardType() /* Accumulator for Reducer */,
				Syntax.WildcardType() /*Input*/}, 
				
				Syntax.NumberType());
	}
	
	public Object report(Argument args[], Context context) throws ExtensionException
	{
		Framework fw = FrameworkFactory.getInstance();
		
		if( fw.isRunning() )
			throw new ExtensionException("MapReduce is allready running. Wait until finish or close NetLogo");
		
		fw.newJob();
		
		try {
			fw.getConfiguration().setMapper(args[0].getString());
			fw.getConfiguration().setReducer(args[1].getString());
			fw.getConfiguration().setAccumulatorFromObj(args[2].get());
			if( args[3].get().getClass() == String.class )
				fw.getConfiguration().setInputDirectory(args[3].getString());
			else if( args[3].get().getClass() == LogoList.class )
			{
				String dir= writeListToFiles(args[3], fw);
				System.out.println("Inputdir: " + dir);
				fw.getConfiguration().setInputDirectory(dir);
			}
			else
				throw new ExtensionException(new FrameworkException("Unsupported argument type " + 
						args[3].get().getClass().getName() + 
						" for parameter input"));
		} catch (LogoException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
		
		String world,model;
		world= Manager.getWorld();
		model= Manager.em.workspace().getModelPath();
		MapReduceRun run;
		if( fw.isMultiNode() )
			run= new MultiNodeRun(fw,world,model);
		else
			run= new SingleNodeRun(fw,world,model);
		fw.setRun(run);
		run.setup();
		run.startRun();
		
		return new Double(fw.getJobNr());
	}

	private String writeListToFiles(Argument argument, Framework fw) throws ExtensionException, LogoException
	{
		String dir= "input-" + fw.getJobNr();
		String sysdir= fw.getSystemFileHandler().addDirectory(dir);
		LogoList input= argument.getList();
		for(int i= 0; i < input.size(); i++)
		{
			Object o= input.get(i);
			if( o.getClass() != LogoList.class )
				throw new ExtensionException(new FrameworkException("Malformated input to MapReduce. List needs to be List of Lists"));
			LogoList kv= (LogoList) o;
			Object key= kv.first();
			Object value= kv.get(1);
			
			System.out.println(i + ": " + key + " " + value);
			
			String fn= fw.getSystemFileHandler().addFile(dir + File.separator + key.toString());
			
			PrintWriter writer;
			try {
				writer = new PrintWriter(fn, "UTF-8");
				if( value.getClass() == LogoList.class )
				{
					LogoList list= (LogoList) value;
					for(int j= 0; j < list.size(); j++)
						writer.println(list.get(j).toString());
				}
				else
					writer.println(value.toString());
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		
		return sysdir;
	}
}
