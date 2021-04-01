package org.apache.solr.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.core.SolrCore;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.ContextImpl;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.EntityProcessor;
import org.apache.solr.handler.dataimport.EntityProcessorWrapper;
import org.apache.solr.handler.dataimport.VariableResolver;
import org.junit.Test;

import com.kmwllc.solr.solrkafka.datasource.KafkaDataSource;
import org.junit.jupiter.api.Disabled;

// import org.apache.solr.handler.dataimport.AbstractDataImportHandlerTestCase;
// public class TestKafkaDataSource extends AbstractDataImportHandlerTestCase {
@Disabled
public class TestKafkaDataSource {

	// TODO: implement me!
	
	
	 @Test
	  public void testBasic() throws Exception {
	    KafkaDataSource dataSource = new KafkaDataSource();
	    Properties p = new Properties();
	    // TODO: kafka stuff
	    p.put("driver", "com.mysql.jdbc.Driver");
	    p.put("url", "jdbc:mysql://127.0.0.1/autos");
	    p.put("user", "root");
	    p.put("password", "");

	    List<Map<String, String>> flds = new ArrayList<>();
	    Map<String, String> f = new HashMap<>();
	    f.put("column", "trim_id");
	    f.put("type", "long");
	    flds.add(f);
	    f = new HashMap<>();
	    f.put("column", "msrp");
	    f.put("type", "float");
	    flds.add(f);

	    // TODO: uncomment ...
	    Context c = getContext(null, null, dataSource, Context.FULL_DUMP, flds, null);
	    dataSource.init(c, p);
	    Iterator<Map<String, Object>> i = dataSource.getData("foo");
	    // This should always be true for kafka I guess
	    if (i.hasNext()) {
	    	Object map  = i.next();
	    	System.out.println("Got next.." + map);
	    }
//	    int count = 0;
//	    Object msrp = null;
//	    Object trim_id = null;
//	    while (i.hasNext()) {
//	      Map<String, Object> map = i.next();
//	      msrp = map.get("msrp");
//	      trim_id = map.get("trim_id");
//	      count++;
//	    }
//	    assertEquals(5, count);
//	    assertEquals(Float.class, msrp.getClass());
//	    assertEquals(Long.class, trim_id.getClass());
	    
	    // Close the datasource properly.
	    dataSource.close();
	  //  assertEquals(true,true);
	    
	    
	  }
	 
	 
	  /**
	   * Helper for creating a Context instance. Useful for testing Transformers
	   * */
	  @SuppressWarnings("unchecked")
	  public static TestContext getContext(EntityProcessorWrapper parent,
	                                   VariableResolver resolver, DataSource parentDataSource,
	                                   String currProcess, final List<Map<String, String>> entityFields,
	                                   final Map<String, String> entityAttrs) {
	    if (resolver == null) resolver = new VariableResolver();
	    final Context delegate = new ContextImpl(parent, resolver,
	            parentDataSource, currProcess,
	        new HashMap<>(), null, null);
	    return new TestContext(entityAttrs, delegate, entityFields, parent == null);
	  }
	   
	  static class TestContext extends Context {
		    private final Map<String, String> entityAttrs;
		    private final Context delegate;
		    private final List<Map<String, String>> entityFields;
		    private final boolean root;
		    String script,scriptlang;

		    public TestContext(Map<String, String> entityAttrs, Context delegate,
		                       List<Map<String, String>> entityFields, boolean root) {
		      this.entityAttrs = entityAttrs;
		      this.delegate = delegate;
		      this.entityFields = entityFields;
		      this.root = root;
		    }

		    @Override
		    public String getEntityAttribute(String name) {
		      return entityAttrs == null ? delegate.getEntityAttribute(name) : entityAttrs.get(name);
		    }

		    @Override
		    public String getResolvedEntityAttribute(String name) {
		      return entityAttrs == null ? delegate.getResolvedEntityAttribute(name) :
		              delegate.getVariableResolver().replaceTokens(entityAttrs.get(name));
		    }

		    @Override
		    public List<Map<String, String>> getAllEntityFields() {
		      return entityFields == null ? delegate.getAllEntityFields()
		              : entityFields;
		    }

		    @Override
		    public VariableResolver getVariableResolver() {
		      return delegate.getVariableResolver();
		    }

		    @Override
		    public DataSource getDataSource() {
		      return delegate.getDataSource();
		    }

		    @Override
		    public boolean isRootEntity() {
		      return root;
		    }

		    @Override
		    public String currentProcess() {
		      return delegate.currentProcess();
		    }

		    @Override
		    public Map<String, Object> getRequestParameters() {
		      return delegate.getRequestParameters();
		    }

		    @Override
		    public EntityProcessor getEntityProcessor() {
		      return null;
		    }

		    @Override
		    public void setSessionAttribute(String name, Object val, String scope) {
		      delegate.setSessionAttribute(name, val, scope);
		    }

		    @Override
		    public Object getSessionAttribute(String name, String scope) {
		      return delegate.getSessionAttribute(name, scope);
		    }

		    @Override
		    public Context getParentContext() {
		      return delegate.getParentContext();
		    }

		    @Override
		    public DataSource getDataSource(String name) {
		      return delegate.getDataSource(name);
		    }

		    @Override
		    public SolrCore getSolrCore() {
		      return delegate.getSolrCore();
		    }

		    @Override
		    public Map<String, Object> getStats() {
		      return delegate.getStats();
		    }


		    @Override
		    public String getScript() {
		      return script == null ? delegate.getScript() : script;
		    }

		    @Override
		    public String getScriptLanguage() {
		      return scriptlang == null ? delegate.getScriptLanguage() : scriptlang;
		    }

		    @Override
		    public void deleteDoc(String id) {

		    }

		    @Override
		    public void deleteDocByQuery(String query) {

		    }

		    @Override
		    public Object resolve(String var) {
		      return delegate.resolve(var);
		    }

		    @Override
		    public String replaceTokens(String template) {
		      return delegate.replaceTokens(template);
		    }
		  }

	  

}
