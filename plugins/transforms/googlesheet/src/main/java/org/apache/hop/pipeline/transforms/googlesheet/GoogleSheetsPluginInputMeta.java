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
package org.apache.hop.pipeline.transforms.googlesheet;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.CheckResult;

import org.apache.hop.core.exception.*;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

import org.apache.hop.core.row.value.ValueMetaString;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;

import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;

import org.apache.hop.metadata.api.IHopMetadataProvider;

import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;


/**
 * Skeleton for PDI Step plugin.
 */

@Transform(
	id = "GoogleSheetsPluginInput",
	image = "GoogleSheetsPluginInput.svg", 
	i18nPackageName = "org.apache.hop.pipeline.transforms.googlesheet.GoogleSheetsPluginInput",
    name = "GoogleSheetsPluginInput.transform.Name",
	description = "GoogleSheetsPluginInput.transform.Name",
	categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
	documentationUrl ="https://github.com/jfmonteil/Pentaho-Google-Sheet-Plugin/blob/master/README.md"
	) 
	
@InjectionSupported( localizationPrefix = "GoogleSheetsPluginInput.injection.", groups = { "SHEET", "INPUT_FIELDS" } )
public class GoogleSheetsPluginInputMeta extends BaseTransformMeta implements ITransformMeta <GoogleSheetsPluginInput, GoogleSheetsPluginInputData>{ //<Constant, ConstantData>
	
	private static Class<?> PKG = GoogleSheetsPluginInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
    
	public  GoogleSheetsPluginInputMeta() {
      super(); // allocate BaseStepMeta
	  allocate(0);
    }
    
	@Injection( name = "jsonCrendentialPath", group = "SHEET" )
    private String jsonCredentialPath;

	@Injection( name = "spreadsheetKey", group = "SHEET" )
    private String spreadsheetKey;
    
	@Injection( name = "worksheetId", group = "SHEET" )
	private String worksheetId;
	
	@Injection( name = "sampleFields", group = "INPUT_Fields" )
	private Integer sampleFields;
	
	@InjectionDeep
	private GoogleSheetsPluginInputFields[] inputFields;
	
    @Override
    public void setDefault() {   
        this.spreadsheetKey = "";
        this.worksheetId = "";  
		this.jsonCredentialPath = "Set path to your "+ "/client_secret.json";
        this.sampleFields=100;
   
   }
	
	public String getJsonCredentialPath() {
        return this.jsonCredentialPath == null ? "" : this.jsonCredentialPath;
    }

    public void setJsonCredentialPath(String key) {
        this.jsonCredentialPath = key;
    }
			
	public GoogleSheetsPluginInputFields[] getInputFields() {
		return inputFields;
	}

    public String getSpreadsheetKey() {
        return this.spreadsheetKey == null ? "" : this.spreadsheetKey;
    }

    public void setSpreadsheetKey(String key) {
        this.spreadsheetKey = key;
    }

    public String getWorksheetId() {
        return this.worksheetId == null ? "" : this.worksheetId;
    }

    public void setWorksheetId(String id) {
        this.worksheetId = id;
    }
    
	public int getSampleFields() {
        return this.sampleFields == null ? 100 : this.sampleFields;
    }

    public void setSampleFields(int sampleFields) {
        this.sampleFields = sampleFields;
    }
	
	public void allocate(int nrfields) {
	    inputFields = new GoogleSheetsPluginInputFields[nrfields];
	}
	

    @Override
    public Object clone() {
        GoogleSheetsPluginInputMeta retval = (GoogleSheetsPluginInputMeta) super.clone();  
        if(retval!=null)
	    {
			int nrKeys = inputFields.length;
			retval.allocate(nrKeys);
			retval.setJsonCredentialPath(this.jsonCredentialPath);
			retval.setSpreadsheetKey(this.spreadsheetKey);
			retval.setWorksheetId(this.worksheetId);
			retval.setSampleFields(this.sampleFields);
			for ( int i = 0; i < nrKeys; i++ ) {
				retval.inputFields[i] = (GoogleSheetsPluginInputFields) inputFields[i].clone();
			}
		}
        return retval;
    }

    @Override
    public String getXml() throws HopException {
        StringBuilder xml = new StringBuilder();
        try {         
            xml.append(XmlHandler.addTagValue("worksheetId", this.worksheetId));
			xml.append(XmlHandler.addTagValue("spreadsheetKey", this.spreadsheetKey));
     		xml.append(XmlHandler.addTagValue("jsonCredentialPath", this.jsonCredentialPath));
			String tmp="100";
			if(this.sampleFields!=null){
				xml.append(XmlHandler.addTagValue("sampleFields", this.sampleFields.toString()));
			}			
            else xml.append(XmlHandler.addTagValue("sampleFields", tmp));
			xml.append(XmlHandler.openTag("fields"));
            for ( int i = 0; i < inputFields.length; i++ ) {
			  GoogleSheetsPluginInputFields field = inputFields[i];
			  xml.append( "      <field>" ).append( Const.CR );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "name", field.getName() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "type", field.getTypeDesc() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "format", field.getFormat() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "currency", field.getCurrencySymbol() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "decimal", field.getDecimalSymbol() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "group", field.getGroupSymbol() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "position", field.getPosition() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "length", field.getLength() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "precision", field.getPrecision() ) );
			  xml.append( "        " ).append( XmlHandler.addTagValue( "trim_type", field.getTrimTypeCode() ) );
			  xml.append( "      </field>" ).append( Const.CR );
			}
			xml.append( "    </fields>" ).append( Const.CR );

        } catch (Exception e) {
            throw new HopValueException("Unable to write step to XML", e);
        }
        return xml.toString();
    }

    @Override
    public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider) throws HopXmlException {
        try {
           
            this.worksheetId = XmlHandler.getTagValue(transformNode, "worksheetId");
            this.spreadsheetKey = XmlHandler.getTagValue(transformNode, "spreadsheetKey");
            this.jsonCredentialPath = XmlHandler.getTagValue(transformNode, "jsonCredentialPath");
			String tmp=XmlHandler.getTagValue(transformNode, "sampleField");
            if(tmp!=null && !tmp.isEmpty()){
			 this.sampleFields = Integer.parseInt(tmp);
			} else {
				this.sampleFields = 100;
			}
            Node fields = XmlHandler.getSubNode(transformNode, "fields");
            int nrfields = XmlHandler.countNodes(fields, "field");

            allocate(nrfields);

            for ( int i = 0; i < nrfields; i++ ) {
				Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );
				GoogleSheetsPluginInputFields field = new GoogleSheetsPluginInputFields();

				field.setName( XmlHandler.getTagValue( fnode, "name" ) );
				field.setType( ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( fnode, "type" ) ) );
				field.setFormat( XmlHandler.getTagValue( fnode, "format" ) );
				field.setCurrencySymbol( XmlHandler.getTagValue( fnode, "currency" ) );
				field.setDecimalSymbol( XmlHandler.getTagValue( fnode, "decimal" ) );
				field.setGroupSymbol( XmlHandler.getTagValue( fnode, "group" ) );
				//field.setNullString( XmlHandler.getTagValue( fnode, "nullif" ) );
				//field.setIfNullValue( XmlHandler.getTagValue( fnode, "ifnull" ) );
				field.setPosition( Const.toInt( XmlHandler.getTagValue( fnode, "position" ), -1 ) );
				field.setLength( Const.toInt( XmlHandler.getTagValue( fnode, "length" ), -1 ) );
				field.setPrecision( Const.toInt( XmlHandler.getTagValue( fnode, "precision" ), -1 ) );
				field.setTrimType( ValueMetaString.getTrimTypeByCode( XmlHandler.getTagValue( fnode, "trim_type" ) ) );
				//field.setRepeated( YES.equalsIgnoreCase( XmlHandler.getTagValue( fnode, "repeat" ) ) );

                inputFields[i] = field;
             }
             super.loadXml( transformNode, metadataProvider );
        } catch (Exception e) {
            throw new HopXmlException("Unable to load step from XML", e);
        }
    }

  /*  @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws HopException {
        try {

            this.worksheetId = rep.getStepAttributeString(id_step, "worksheetId");
            this.spreadsheetKey = rep.getStepAttributeString(id_step, "spreadsheetKey");
            this.jsonCredentialPath = rep.getStepAttributeString(id_step, "jsonCredentialPath");
			this.sampleFields =(int) rep.getStepAttributeInteger(id_step, "sampleFields");
            int nrfields = rep.countNrStepAttributes(id_step, "field_name");

            allocate(nrfields);

            for ( int i = 0; i < nrfields; i++ ) {
				GoogleSheetsPluginInputFields field = new GoogleSheetsPluginInputFields();

				field.setName( rep.getStepAttributeString( id_step, i, "field_name" ) );
				field.setType( ValueMetaFactory.getIdForValueMeta( rep.getStepAttributeString( id_step, i, "field_type" ) ) );
				field.setFormat( rep.getStepAttributeString( id_step, i, "field_format" ) );
				field.setCurrencySymbol( rep.getStepAttributeString( id_step, i, "field_currency" ) );
				field.setDecimalSymbol( rep.getStepAttributeString( id_step, i, "field_decimal" ) );
				field.setGroupSymbol( rep.getStepAttributeString( id_step, i, "field_group" ) );
				//field.setNullString( rep.getStepAttributeString( id_step, i, "field_nullif" ) );
				//field.setIfNullValue( rep.getStepAttributeString( id_step, i, "field_ifnull" ) );
				field.setPosition( (int) rep.getStepAttributeInteger( id_step, i, "field_position" ) );
				field.setLength( (int) rep.getStepAttributeInteger( id_step, i, "field_length" ) );
				field.setPrecision( (int) rep.getStepAttributeInteger( id_step, i, "field_precision" ) );
				field.setTrimType( ValueMetaString.getTrimTypeByCode( rep.getStepAttributeString( id_step, i, "field_trim_type" ) ) );
				//field.setRepeated( rep.getStepAttributeBoolean( id_step, i, "field_repeat" ) );

				inputFields[i] = field;
			  }
        } catch (Exception e) {
            throw new HopException("Unexpected error reading step information from the repository", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws HopException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, "spreadsheetKey", this.spreadsheetKey);
            rep.saveStepAttribute(id_transformation, id_step, "worksheetId", this.worksheetId);
            rep.saveStepAttribute(id_transformation, id_step, "jsonCredentialPath", this.jsonCredentialPath);
            rep.saveStepAttribute(id_transformation, id_step, "sampleFields", this.sampleFields.toString());

		    int nrfields = rep.countNrStepAttributes(id_step, "field_name");
           
		for ( int i = 0; i < inputFields.length; i++ ) {
			GoogleSheetsPluginInputFields field = inputFields[i];

			rep.saveStepAttribute( id_transformation, id_step, i, "field_name", field.getName() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_type", field.getTypeDesc() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_format", field.getFormat() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_currency", field.getCurrencySymbol() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_decimal", field.getDecimalSymbol() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_group", field.getGroupSymbol() );
			//rep.saveStepAttribute( id_transformation, id_step, i, "field_nullif", field.getNullString() );
			//rep.saveStepAttribute( id_transformation, id_step, i, "field_ifnull", field.getIfNullValue() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_position", field.getPosition() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_length", field.getLength() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_precision", field.getPrecision() );
			rep.saveStepAttribute( id_transformation, id_step, i, "field_trim_type", field.getTrimTypeCode() );
			//rep.saveStepAttribute( id_transformation, id_step, i, "field_repeat", field.isRepeated() );
		  }
        } catch (Exception e) {
            throw new HopException("Unable to save step information to the repository for id_step=" + id_step, e);
        }
    }*/

    @Override
    public void getFields(IRowMeta rowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider) throws HopTransformException {
        try {
             rowMeta.clear(); // Start with a clean slate, eats the input
             for ( int i = 0; i < inputFields.length; i++ ) {
			  GoogleSheetsPluginInputFields field = inputFields[i];

			  int type = field.getType();
			  if ( type == 0 ) {/*ValueMetaInterface.TYPE_NONE*/
				type = 2;//ValueMetaInterface.TYPE_STRING;
			  }

			  try {
				IValueMeta v = ValueMetaFactory.createValueMeta( field.getName(), type );
				
				v.setLength( field.getLength() );
				v.setPrecision( field.getPrecision() );
				v.setOrigin( name );
				v.setConversionMask( field.getFormat() );
				v.setDecimalSymbol( field.getDecimalSymbol() );
				v.setGroupingSymbol( field.getGroupSymbol() );
				v.setCurrencySymbol( field.getCurrencySymbol() );
				//v.setDateFormatLenient( dateFormatLenient );
				//v.setDateFormatLocale( dateFormatLocale );
				v.setTrimType( field.getTrimType() );

				rowMeta.addValueMeta( v );
			  } catch ( Exception e ) {
				throw new HopTransformException( e );
			  }
			}
        } catch (Exception e) {

        }
    }

    @Override
    public void check(List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables space,IHopMetadataProvider metadataProvider) {
        if (prev == null || prev.size() == 0) {
            remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_OK, "Not receiving any fields from previous steps.", transformMeta));
        } else {
            remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_ERROR, String.format("Step is connected to previous one, receiving %1$d fields", prev.size()), transformMeta));
        }

        if (input.length > 0) {
            remarks.add( new CheckResult(ICheckResult.TYPE_RESULT_ERROR, "Step is receiving info from other steps!", transformMeta) );
        } else {
            remarks.add(new CheckResult(ICheckResult.TYPE_RESULT_OK, "No input received from other steps.", transformMeta));
        }
    }
  
  @Override
  public GoogleSheetsPluginInput createTransform( TransformMeta transformMeta, GoogleSheetsPluginInputData iTransformData, int cnr, PipelineMeta pm, Pipeline pipeline ) {
    return new GoogleSheetsPluginInput( transformMeta,this,iTransformData, cnr, pm, pipeline );
  }
  @Override
  public GoogleSheetsPluginInputData getTransformData() {
    return new GoogleSheetsPluginInputData();
  }
  
   @Override public String getDialogClassName() {
    return GoogleSheetsPluginInputDialog.class.getName();
  }
  /*public Neo4Jtput createTransform( TransformMeta transformMeta, Neo4JOutputData iTransformData, int cnr, PipelineMeta pipelineMeta, Pipeline disp ) {
    return new Neo4JOutput( transformMeta, this, iTransformData, cnr, pipelineMeta, disp );
  }

  public Neo4JOutputData getTransformData() {
    return new Neo4JOutputData();
  }*/
 
  
  
}

