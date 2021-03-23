package org.apache.hop.pipeline.transforms.couchbase;


import org.w3c.dom.Node;


import org.apache.hop.core.annotations.Transform;

import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;

import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.variables.IVariables;


@Transform(
  id = "CouchbaseOutput",
  name = "Couchbase Output",
  description = "Read data from couchbase",
  image = "CouchbaseOutput.svg",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output"
)
@InjectionSupported( localizationPrefix = "couchbase.Injection.", groups = { "PARAMETERS", "RETURNS" } )
public class CouchbaseOutputMeta extends BaseTransformMeta implements ITransformMeta<CouchbaseOutput,CouchbaseOutputData> {

  public static Class<?> PKG = CouchbaseOutput.class;

  public static final String CONNECTION = "connection";
  public static final String COLLECTION = "collection";
  public static final String INSERTTYPE = "inserttype";
  public static final String KEY = "key";
  public static final String VALUE = "value";


  public CouchbaseOutput createTransform( TransformMeta transformMeta, CouchbaseOutputData iTransformData, int cnr, PipelineMeta pipelineMeta, Pipeline disp ) {
    return new CouchbaseOutput( transformMeta, this, iTransformData, cnr, pipelineMeta, disp );
  }


  @Injection( name = CONNECTION )
  private String connectionName;

  @Injection( name = COLLECTION )
  private String collection;

  @Injection( name = INSERTTYPE )
  private String inserttype;

  @Injection( name = KEY )
  private String key;

  @Injection( name = VALUE )
  private String value;


  @Override public void setDefault() {
    collection = "default";
  }


  @Override public CouchbaseOutputData getTransformData() {
    return new CouchbaseOutputData();
  }

  public String getDialogClassName() {
    return CouchbaseOutputDialog.class.getName();
  }

  public ITransformDialog getDialog( Shell shell, IVariables variables, ITransformMeta meta, PipelineMeta pipelineMeta, String name ) {
    return new CouchbaseOutputDialog( shell, variables, meta, pipelineMeta, name );
  }


  @Override public String getXml() {
    StringBuilder xml = new StringBuilder();
    xml.append( XmlHandler.addTagValue( CONNECTION, connectionName ) );
	xml.append( XmlHandler.addTagValue( COLLECTION, collection ) );
	xml.append( XmlHandler.addTagValue( INSERTTYPE, inserttype ) );
	xml.append( XmlHandler.addTagValue( KEY, key ) );
	xml.append( XmlHandler.addTagValue( VALUE, value ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    connectionName = XmlHandler.getTagValue(transformNode, CONNECTION);
    collection = XmlHandler.getTagValue(transformNode, COLLECTION);
    inserttype = XmlHandler.getTagValue(transformNode, INSERTTYPE);
    key = XmlHandler.getTagValue(transformNode, KEY);
    value = XmlHandler.getTagValue(transformNode, VALUE);
  }


  public String getConnection() {
    return connectionName;
  }


  public void setConnection(String connectionName) {
    this.connectionName = connectionName;
  }

  public String getCollection() {
    return collection;
  }


  public void setCollection( String collection ) {
    this.collection = collection;
  }


  public String getInsertType() {
    return inserttype;
  }

  public void setInsertType( String inserttype ) {
    this.inserttype = inserttype;
  }
  

  public String getKey() {
    return key;
  }
  

  public void setKey( String key ) {
    this.key = key;
  }


  public void setValue( String value ) {
    this.value = value;
  }

  
  public String getValue() {
    return value;
  }


}


