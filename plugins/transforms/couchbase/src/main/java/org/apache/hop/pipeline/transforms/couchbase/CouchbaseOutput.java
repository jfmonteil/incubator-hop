/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hop.pipeline.transforms.couchbase;

import org.apache.commons.lang.StringUtils;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;

import java.util.List;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import com.couchbase.client.java.*;
import com.couchbase.client.java.kv.*;
import com.couchbase.client.java.json.*;
import org.apache.hop.pipeline.transform.BaseTransform;

import org.apache.hop.pipeline.transforms.couchbase.connection.CouchbaseConnection;


/**
 * Describe your step plugin.
 * 
 */

public class CouchbaseOutput extends BaseTransform<CouchbaseOutputMeta, CouchbaseOutputData> implements ITransform<CouchbaseOutputMeta, CouchbaseOutputData> {

  private static Class<?> PKG = CouchbaseOutput.class; // for i18n purposes, needed by Translator2!!
  

	public CouchbaseOutput(TransformMeta s, CouchbaseOutputMeta meta, CouchbaseOutputData data, int c, PipelineMeta t, Pipeline dis) {
		super(s,meta,data,c,t,dis);
	}
  

   @Override
   public boolean init() {
        

		if ( meta==null || StringUtils.isEmpty( meta.getConnection() ) ) {
		  log.logError( "You need to specify a Couchbase Connection connection to use in this step" );
		  return false;
		}

        
		if (super.init()) {
            try {

			  data.couchbaseConnection = metadataProvider.getSerializer( CouchbaseConnection.class ).load( meta.getConnection() );
              Collection collection=data.couchbaseConnection.connectToCouchbase(this);
			  if (collection==null)
			  {
				  logError("No collection");
				  return false;
			  }
			  data.collection=collection;
			 

            } catch (Exception e) {
                logError("Error: for couchbase Database : on  "+data.couchbaseConnection.getIsCloud()+" ,Collection :"+resolve(meta.getCollection())+" for Bucket :"+data.couchbaseConnection.getRealBucket(this) +"exception"+ e.getMessage(), e);
                setErrors(1L);
                stopAll();
                return false;
            }

            return true;
        }
        return false;
    }
   

  public boolean processRow() throws HopException {

	Object[] row = getRow();
	int numErrors = 0;
    if (first) {
		if(row==null){
			logError( "Error : No rows" ); //$NON-NLS-1$
			return false;
	    }
		first = false;
	    logDetailed("First");
		data.outputRowMeta=getInputRowMeta().clone();
		if(data.outputRowMeta==null){
		logError( "Error : no inbound fields "); //$NON-NLS-1$
			numErrors++;
	    }	
		meta.getFields(data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);
		
		logDetailed("Checking data : key : "+meta.getKey()+" value :"+meta.getValue());
		//Getting key and value
		String keyField=meta.getKey();
		String valueField=meta.getValue();
		
		if ( ( keyField.isEmpty() ) || valueField.isEmpty() )  {
			logError( "Field Name Is Null" ) ; //$NON-NLS-1$
			numErrors++;
	    }
		data.m_valueFieldNr = data.outputRowMeta.indexOfValue( valueField );
	    data.m_keyFieldNr = data.outputRowMeta.indexOfValue( keyField );
		if ( data.m_valueFieldNr < 0 || data.m_keyFieldNr<0) {
        logError( "Couldnt Find Field " +keyField +" "+valueField ); //$NON-NLS-1$
        numErrors++;
		if ( numErrors > 0 ) {
			logError("too many errors");
			setErrors( numErrors );
			stopAll();
        return false;
        }
      }
	}
    if(!first && row==null){
			logBasic( "No More Rows"); //$NON-NLS-1$
			setOutputDone();
			return false;
	   }
     
	  logDetailed("row :"+data.currentRow);
	  logBasic("Insert type"+meta.getInsertType());
	  /*try{*/
		MutationResult result = null;		
		String rawValue = row[data.m_valueFieldNr].toString();
		String rawKey = row[data.m_keyFieldNr].toString();
		logDetailed("data inbound value:"+rawValue+ "for key:"+rawKey+" Optype="+meta.getInsertType());
		JsonObject content=JsonObject.fromJson(rawValue);
		if(meta.getInsertType()=="Upsert"){

			 try{

			 result = data.collection.upsert(rawKey, content);
			 } catch (Exception ex) {
				logError("Upsert Failed ! "+ex);
				return false;
						
			 }
		}
		else if(meta.getInsertType()=="Insert"){


			 try{
			 result = data.collection.insert(rawKey, content);
			 } catch (Exception ex) {
				logError("Inseret failed"+ex);
				return false;
						
				}
		}
		else{
			
			 try{

			 result = data.collection.upsert(rawKey, content);
			 } catch (Exception ex) {
				logError("Upsert by default Failed ! "+ex);
				return false;
						
			 }
		}
	  data.currentRow++;

    
    return true;
  }
   private Object getRowDataValue(final IValueMeta targetValueMeta, final IValueMeta sourceValueMeta, final Object value, final DateFormat df,final DateTimeFormatter f) throws HopException
    {
        if (value == null) {
            return value;
        }

        if (IValueMeta.TYPE_TIMESTAMP  == targetValueMeta.getType()) {
			try{

			LocalDateTime localDateTime = LocalDateTime.from(f.parse(value.toString()));
			Timestamp timestamp = Timestamp.valueOf(localDateTime);
			return targetValueMeta.convertData(sourceValueMeta, timestamp);

			
			} catch (ClassCastException exc) {
            logError("Timestamp class cast exeption");
            return targetValueMeta.convertData(sourceValueMeta, value.toString());			
			}
        }
		if (IValueMeta.TYPE_STRING == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, value.toString());
        }
        
        if (IValueMeta.TYPE_NUMBER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Double.valueOf(value.toString()));
        }
        
        if (IValueMeta.TYPE_INTEGER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Long.valueOf(value.toString()));
        }
        
        if (IValueMeta.TYPE_BIGNUMBER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, new BigDecimal(value.toString()));
        }
        
        if (IValueMeta.TYPE_BOOLEAN == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Boolean.valueOf(value.toString()));
        }
        
        if (IValueMeta.TYPE_BINARY == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, value);
        }

        if (IValueMeta.TYPE_DATE == targetValueMeta.getType()) {
            try {
                return targetValueMeta.convertData(sourceValueMeta, df.parse(value.toString()));
            } catch (final ParseException e) {
                throw new HopValueException("Unable to convert data type of value");
            }
        }

        throw new HopValueException("Unable to convert data type of value");
    }

}