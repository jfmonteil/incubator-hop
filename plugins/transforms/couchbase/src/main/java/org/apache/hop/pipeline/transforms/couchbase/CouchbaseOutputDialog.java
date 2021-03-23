package org.apache.hop.pipeline.transforms.couchbase;


import org.apache.commons.lang.StringUtils;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;

import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;

import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.widgets.Button;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

import org.eclipse.swt.widgets.Text;

import com.couchbase.client.java.*;

import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import org.apache.hop.core.Const;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;

import org.apache.hop.core.variables.IVariables;

import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.pipeline.transforms.couchbase.connection.CouchbaseConnection;



import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

public class CouchbaseOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = CouchbaseOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;
  
  private CCombo wCollection;
  private MetaSelectionLine<CouchbaseConnection> wConnection;
  private CCombo wInsertType;
  private CCombo wKey;
  private CCombo wValue;


  private CouchbaseOutputMeta input;

  private boolean gotPreviousFields = false;


  public CouchbaseOutputDialog( Shell parent, IVariables v,Object inputMetadata, PipelineMeta pipelineMeta, String s ) {
    super( parent,v, (BaseTransformMeta) inputMetadata, pipelineMeta, s );
    input = (CouchbaseOutputMeta) inputMetadata;

  
    metadataProvider = HopGui.getInstance().getMetadataProvider();
   
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    FormLayout shellLayout = new FormLayout();
    shell.setLayout( shellLayout );
    shell.setText( "Couchbase Output" );
			
    ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
    };
    changed = input.hasChanged();


    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( "Step name" );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( wlTransformName, 0, SWT.CENTER );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    wConnection =new MetaSelectionLine<>(
            variables,
            metadataProvider,
            CouchbaseConnection.class,
            shell,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            "Couchbase Connection",
            "The name of the Couchbase connection to use");
    if(wConnection==null)
    {
        logError("Error displaying connections", "Error dis");

    }
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( 0, 0 );
    fdConnection.right = new FormAttachment(100, -0 );
    fdConnection.top = new FormAttachment( lastControl, margin );
    wConnection.setLayoutData( fdConnection );
    try {
      wConnection.fillItems();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting list of connections", e);
    }
    lastControl = wConnection;
	
	 // collection line
    //Label
    Label wlcollection = new Label( shell, SWT.RIGHT );
    wlcollection.setText( "Collection" );//collection
    props.setLook( wlcollection );
    FormData fdlcollection = new FormData();
    fdlcollection.left = new FormAttachment( 0, 0 );
    fdlcollection.right = new FormAttachment( middle, -margin );
    fdlcollection.top = new FormAttachment( lastControl, margin );
    wlcollection.setLayoutData( fdlcollection ); 
	//Collectionbutton
    Button getCollectionButton = new Button(shell, SWT.PUSH | SWT.CENTER);
	getCollectionButton.setText("Get collections");
	props.setLook(getCollectionButton);
	FormData getCollectionButtonData = new FormData();
	getCollectionButtonData.top = new FormAttachment(lastControl, margin);
	getCollectionButtonData.right = new FormAttachment(100, 0);
	getCollectionButton.setLayoutData(getCollectionButtonData);
	//textfield
    wCollection = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCollection );
    wCollection.addModifyListener( lsMod );
    FormData fdcollection = new FormData();
    fdcollection.left = new FormAttachment( middle, 0 );
    fdcollection.top = new FormAttachment( lastControl, margin );
    fdcollection.right = new FormAttachment( getCollectionButton, 0 );
    wCollection.setLayoutData( fdcollection );
    lastControl = wCollection;
	
	
	
	//Insert/upsert type
	Label wlInsertType = new Label( shell, SWT.RIGHT );
    wlInsertType.setText( "Insert type" );
    props.setLook( wlInsertType );
    FormData fdlInsertType = new FormData();
    fdlInsertType.left = new FormAttachment( 0, 0 );
    fdlInsertType.right = new FormAttachment( middle, -margin );
    fdlInsertType.top = new FormAttachment( lastControl, 2 * margin );
    wlInsertType.setLayoutData( fdlInsertType );
	wInsertType = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInsertType );
    wInsertType.addModifyListener( lsMod );
    FormData fdInsertType = new FormData();
    fdInsertType.left = new FormAttachment( middle, 0 );
    fdInsertType.right = new FormAttachment(100, 0 );
    fdInsertType.top = new FormAttachment( wlInsertType, 0, SWT.CENTER );
    wInsertType.setLayoutData( fdInsertType );
    lastControl = wInsertType;
	
	//Key
	Label wlKey = new Label( shell, SWT.RIGHT );
    wlKey.setText( "Key" );
    props.setLook( wlKey );
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment( 0, 0 );
    fdlKey.right = new FormAttachment( middle, -margin );
    fdlKey.top = new FormAttachment( lastControl, 2 * margin );
    wlKey.setLayoutData( fdlKey );
	wKey = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKey );
    wKey.addModifyListener( lsMod );
    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment( middle, 0 );
    fdKey.right = new FormAttachment(100, 0 );
    fdKey.top = new FormAttachment( wlKey, 0, SWT.CENTER );
    wKey.setLayoutData( fdKey );
    lastControl = wKey;
	
	//Value
	
	Label wlValue = new Label( shell, SWT.RIGHT );
    wlValue.setText( "Value" );
    props.setLook( wlValue );
    FormData fdlValue = new FormData();
    fdlValue.left = new FormAttachment( 0, 0 );
    fdlValue.right = new FormAttachment( middle, -margin );
    fdlValue.top = new FormAttachment( lastControl, 2 * margin );
    wlValue.setLayoutData( fdlValue );
	wValue = new CCombo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValue );
    wValue.addModifyListener( lsMod );
    FormData fdValue = new FormData();
    fdValue.left = new FormAttachment( middle, 0 );
    fdValue.right = new FormAttachment(100, 0 );
    fdValue.top = new FormAttachment( wlValue, 0, SWT.CENTER );
     wValue.setLayoutData( fdValue );
    lastControl = wValue;
	
	//Build JSON from fields
	/*Label wlValueFields = new Label( shell, SWT.LEFT );
    wlValueFields.setText( "or Build JSON Value from fields" );
    props.setLook( wlValueFields );
    FormData fdlValueFields = new FormData();
    fdlValueFields.left = new FormAttachment( 0, 0 );
    fdlValueFields.right = new FormAttachment( middle, -margin );
    fdlValueFields.top = new FormAttachment( lastControl, margin );
	wlValueFields.setLayoutData( fdlValueFields );
	 ColumnInfo[] valueFieldsColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Couchbase name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
    };
	
	wValueFields = new TableView( pipelineMeta, shell, SWT.FULL_SELECTION | SWT.MULTI, returnColumns, input.getReturnValues().size(), lsMod, props );
    props.setLook( wValueFields );
    wValueFields.addModifyListener( lsMod );
    FormData fdValueFields = new FormData();
    fdValueFields.left = new FormAttachment( 0, 0 );
    fdValueFields.right = new FormAttachment( wbGetReturnFields, 0 );
    fdValueFields.top = new FormAttachment( wlValueFields, margin );
    fdValueFields.bottom = new FormAttachment( wlValueFields, 300 + margin );
    wValueFields.setLayoutData( fdValueFields );
    lastControl = wValueFields;*/
	
	
    shell.pack();
    Rectangle bounds = shell.getBounds();

    /*wScrolledComposite.setContent( shell );
    wScrolledComposite.setExpandHorizontal( true );
    wScrolledComposite.setExpandVertical( true );
    wScrolledComposite.setMinWidth( bounds.width );
    wScrolledComposite.setMinHeight( bounds.height );*/


    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOk,wCancel }, margin, null );

	
	// Add listeners
    //
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wOk.addListener( SWT.Selection, e -> ok() );

    wConnection.addModifyListener( lsMod );
	wCollection.addModifyListener( lsMod );
	wInsertType.addModifyListener( lsMod );
	wKey.addModifyListener( lsMod );
	wValue.addModifyListener( lsMod );
    wTransformName.addModifyListener( lsMod );
	
		
	// Chose collection Button
	/*collectionButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {    				
   				try {
						Couchbase couchbase;
						metadataProviderFactory<CouchbaseConnection> factory = CouchbaseConnectionUtil.getConnectionFactory(metadataProvider);
						CouchbaseConnection couchbaseConnection = factory.loadElement(wConnection.getText());
						couchbaseConnection.initializeVariablesFrom(pipelineMeta);					
						String query="SHOW collectionS";						
						couchbase=couchbaseConnection.connectToCouchbase();
						
						QueryResult result = couchbase.query(new Query(query));
						if(result!=null)
						{
							List<List<Object>> collectionNames = result.getResults().get(0).getSeries().get(0).getValues();
									
							//List<String> collections = Lists.newArrayList();
							int selectedcollection= -1;
							int i=0;
							if (collectionNames != null) 
							{
								String[] collectionsList=new String[collectionNames.size()];

								for (List<Object> collection : collectionNames) {
									collectionsList[i]=(collection.get(0).toString());
									if(wCollection!=null && !wCollection.getText().isEmpty() && collectionsList[i].equals(wCollection.getText())){
										selectedcollection = i;	
									}
									i++;	
								}
								
								EnterSelectionDialog esd = new EnterSelectionDialog(shell, collectionsList, "collections", "Select a collection.");
								if (selectedcollection > -1) {
									esd.setSelectedNrs(new int[]{selectedcollection});
								}
								String s=esd.open();
								if(s!=null)
								{
									if (esd.getSelectionIndeces().length > 0) {
										selectedcollection = esd.getSelectionIndeces()[0];
										String db = collectionsList[selectedcollection];
										if(db!=null){
											wCollection.setText(db);
										}										
									} 
									else {
										wCollection.setText("");
									}
								}
								
							}
						}
						couchbase.close();
					} catch(Exception ex) {
					        new ErrorDialog( shell, "Error", "Error retrieving collections",ex );
					}
			}
	});*/
   
	getCollectionButton.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        setConnectionValues();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;

  }


  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }
  
  private IRowMeta getPreviousFields() {
    IRowMeta previousFields=null;
    try {
      previousFields = pipelineMeta.getPrevTransformFields(variables,transformName);
    } catch ( HopTransformException e ) {
      new ErrorDialog(shell, BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage"),e );
      //previousFields = new IRowMeta();
    }

    return previousFields;
  }
  
  public List<String> getCollections()
  {
	 List<String> collectionsNames = new ArrayList<String>(); 
	 //CouchbaseOutputMeta oneMeta = new CouchbaseOutputMeta();
     //this.getInfo( oneMeta );
	try{
   	 /*TOCHANGE metadataProviderFactory<CouchbaseConnection> factory = CouchbaseConnectionUtil.getConnectionFactory( metadataProvider );
     CouchbaseConnection couchbaseConnection = factory.loadElement( wConnection.getText() );
     couchbaseConnection.initializeVariablesFrom( pipelineMeta );
     couchbaseConnection.connectToCouchbaseBucket();*/

	 Bucket bucket=null;

	
	if(bucket==null)
	 {
		 logBasic("Impossible to get collections, bucket null");
		 return null;
	 }
	 
	 CollectionManager collectionManager = bucket.collections();
	
	  for (ScopeSpec scope : collectionManager.getAllScopes()) {
		//System.out.println("  Scope: " + scope.name());
        logBasic("found scope,"+scope.name());
		for (CollectionSpec collection : scope.collections()) {
		  logBasic("found collection,"+collection.name());
		  collectionsNames.add(scope.name() + collection.name());
		}
	  }
	 } catch (Exception ex) {
	     new ErrorDialog( shell, BaseMessages
          .getString( org.apache.hop.pipeline.transforms.couchbase.CouchbaseOutputMeta.PKG,
              "System.Dialog.Error.Title" ), BaseMessages
          .getString( org.apache.hop.pipeline.transforms.couchbase.CouchbaseOutputMeta.PKG,
              "CouchbaseDialog.ErrorDialog.getcollections.Message" ), ex );
			  return null;
	 }
	 return collectionsNames;
	 
	/* data.metadataProvider = metadataProviderUtil.findmetadataProvider( this );
	 couchbaseConnection = CouchbaseConnectionUtil.getConnectionFactory(data.metadataProvider).loadElement(environmentSubstitute(meta.getConnectionName()));
	 couchbaseConnection.initializeVariablesFrom(this);	*/
	 
  }


  public void getData() {

    wTransformName.setText( Const.NVL( transformName, "" ) );
    wConnection.setText( Const.NVL( input.getConnection(), "" ) );
    wCollection.setText(Const.NVL(input.getCollection(),"default"));
	wInsertType.setText(Const.NVL(input.getInsertType(),"Upsert"));
	wValue.setText(Const.NVL(input.getValue(),""));
	wKey.setText(Const.NVL(input.getKey(),""));
   
    // List of connections...
    //
    /*try {
      
      List<String> elementNames = CouchbaseConnectionUtil.getConnectionFactory( metadataProvider ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Couchbase connections", e );
    }*/
   

	//List of Inseret types...
	 List<String> elementNames = new ArrayList<String>();
     elementNames.add("Upsert");
	 elementNames.add("Insert");
	 elementNames.add("Batch - NA");
     wInsertType.setItems( elementNames.toArray( new String[ 0 ] ) );

    List<String> keyNames = new ArrayList<String>();
	List<String> valueNames = new ArrayList<String>();
	if ( !gotPreviousFields ) {
      try {
        String key = wKey.getText();
        String value = wValue.getText();
        IRowMeta r = getPreviousFields(  );
        if ( r != null ) {
          wKey.setItems( r.getFieldNames() );
          wValue.setItems( r.getFieldNames() );
        }
      
      } catch ( Exception ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "TableOutputDialog.FailedToGetFields.DialogTitle" ), BaseMessages
            .getString( PKG, "TableOutputDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  
  private void ok() {
    if ( StringUtils.isEmpty( wTransformName.getText() ) ) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo( input );
    dispose();
  }
  
  private void setConnectionValues(){
	  	try {
		List<String> collectionNames=getCollections();
		if(collectionNames!=null){
		wCollection.setItems( collectionNames.toArray( new String[ 0 ] ) );
		}
		else throw new HopException( "No collections");
	
	   } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Couchbase collections", e );
    }
  }
  
  private void getInfo( CouchbaseOutputMeta meta ) {
    meta.setConnection( wConnection.getText() );
    //meta.setQuery( wQuery.getText() );
	meta.setCollection(wCollection.getText());
	meta.setInsertType(wInsertType.getText());
	meta.setKey(wKey.getText());
	meta.setValue(wValue.getText());
	
	//meta.setVariables(wVariables.getSelection());

    /*List<ReturnValue> returnValues = new ArrayList<>();
    for ( int i = 0; i < wReturns.nrNonEmpty(); i++ ) {
      TableItem item = wReturns.getNonEmpty( i );
      String name = item.getText( 1 );
      String couchbaseName = item.getText( 2 );
      /*String type = item.getText( 3 );
      int length = Const.toInt( item.getText( 4 ), -1 );
      String format = item.getText( 5 );
      returnValues.add( new ReturnValue( name, couchbaseName) );
    }
    meta.setReturnValues( returnValues );*/
  }


  /*private synchronized void preview() {
    CouchbaseOutputMeta oneMeta = new CouchbaseOutputMeta();
    this.getInfo( oneMeta );
    pipelineMeta previewMeta = TransPreviewFactory.generatePreviewTransformation( this.pipelineMeta, oneMeta, this.wTransformName.getText() );
    this.pipelineMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    previewMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    EnterNumberDialog
      numberDialog = new EnterNumberDialog( this.shell, this.props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogTitle" ),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogMessage" )
    );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog( this.shell, previewMeta, new String[] { this.wTransformName.getText() }, new int[] { previewSize } );
      progressDialog.open();
      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();
      if ( !progressDialog.isCancelled() && trans.getResult() != null && trans.getResult().getNrErrors() > 0L ) {
        EnterTextDialog etd = new EnterTextDialog( this.shell,
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title", new String[ 0 ] ),
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message", new String[ 0 ] ), loggingText, true );
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd = new PreviewRowsDialog( this.shell, this.pipelineMeta, 0, this.wTransformName.getText(), progressDialog.getPreviewRowsMeta( this.wTransformName.getText() ),
        progressDialog.getPreviewRows( this.wTransformName.getText() ), loggingText );
      prd.open();
    }
  }*/
  
  private String converType(String initType){
	  
      String destType="String";
	  if(initType==null || initType.isEmpty())
		  return destType;
	  switch(initType.toLowerCase())
	  {
		case "float" : destType ="Number"; break;
		case "boolean" : destType ="Boolean"; break;
		case "integer" : destType= "Integer"; break;
        case "string" : destType = "String"; break;
	  }	
      return destType;	  
  }

    /*private void getReturnValues() throws HopException {

    try {

		  metadataProviderFactory<CouchbaseConnection> factory = CouchbaseConnectionUtil.getConnectionFactory( metadataProvider );
		  CouchbaseConnection couchbaseConnection = factory.loadElement(this.pipelineMeta.environmentSubstitute(wConnection.getText()));
		  couchbaseConnection.initializeVariablesFrom( this.pipelineMeta );
		  Couchbase couchbase;  
		  String query=this.pipelineMeta.environmentSubstitute(wQuery.getText());
          		  
		  String collection=this.pipelineMeta.environmentSubstitute(wCollection.getText());
		  couchbase=couchbaseConnection.connectToCouchbase();
		  //couchbase.setcollection(collection);
		  
		  //Working on query to extract tags, columns
		  if(query!=null && !query.isEmpty())
		  {
			  TableItem itemTime = new TableItem(wReturns.table, SWT.NONE);
              itemTime.setText(1, "time");
			  itemTime.setText(2, "Time");
			  itemTime.setText(3, "Timestamp");
			  itemTime.setText(4, "");
			  itemTime.setText(5, "yyyy-MM-dd'T'HH:mm:ss'Z'");			  
			 
			  int startPos=query.toLowerCase().indexOf("from");
			  String showTagsQuery="SHOW TAG KEYS ON "+collection+" "+query.substring(startPos);
			  
			  QueryResult result = couchbase.query(new Query(showTagsQuery));
			  List<List<Object>> columns = result.getResults().get(0).getSeries().get(0).getValues();

			  if (columns != null) 
				{
					for (List<Object> column : columns) {
							if(column!=null && column.get(0)!=null && !column.get(0).toString().isEmpty())
							{
								TableItem item = new TableItem(wReturns.table, SWT.NONE);
								item.setText(1, column.get(0).toString());
								item.setText(2, "Tag");
								item.setText(3, "String");
								item.setText(4, "");
								item.setText(5, "");
							}
						}
						
				}
			 String showFieldsQuery="SHOW FIELD KEYS ON "+collection+" "+query.substring(startPos);
			 result = couchbase.query(new Query(showFieldsQuery));
			 List<List<Object>> values = result.getResults().get(0).getSeries().get(0).getValues();
			 
			 if (values != null) 
				{
					for (List<Object> fields : values) {
						if(columns!=null && !columns.isEmpty()){
							TableItem itemF = new TableItem(wReturns.table, SWT.NONE);
							itemF.setText(1, fields.get(0).toString());
							itemF.setText(2, "Field");
							itemF.setText(3, converType(fields.get(1).toString()));
							itemF.setText(4, "");
							itemF.setText(5, "");
							}
						}
						
				}
		  }
		  /*Map<String, Object> tags = new HashMap<String, Object>(); 
		  if(result.getResults().get(0).getSeries().get(0).getTags()!=null) {
			tags.putAll(result.getResults().get(0).getSeries().get(0).getTags());
          }

		  if (tags != null) 
			{
				for (Map.Entry<String, Object> entry : tags.entrySet()) {
					//System.out.println("Item : " + entry.getKey() + " Count : " + entry.getValue());
					TableItem item = new TableItem(wReturns.table, SWT.NONE);
					item.setText(1, entry.getKey());
					item.setText(2, "Tag");
					item.setText(3, "String");
				}											
			}
		 
		 couchbase.close();
		
		
		} catch(Exception e) {
		  throw new HopException( "Error connecting to Couchbase connection to get collections", e );
		}
	  

    
        
     }*/
	 



}
