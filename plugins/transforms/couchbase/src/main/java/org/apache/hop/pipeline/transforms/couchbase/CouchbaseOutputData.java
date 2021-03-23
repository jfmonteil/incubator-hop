package org.apache.hop.pipeline.transforms.couchbase;

import java.util.List;

import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transforms.couchbase.connection.CouchbaseConnection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import com.couchbase.client.java.*;


public class CouchbaseOutputData extends BaseTransformData implements ITransformData {

  public List<List<Object>> rows;
  
  public IRowMeta outputRowMeta;
  public CouchbaseConnection couchbaseConnection;
  public Collection collection;

  public int m_valueFieldNr;
  public int m_keyFieldNr;


  public int currentRow=0;

}