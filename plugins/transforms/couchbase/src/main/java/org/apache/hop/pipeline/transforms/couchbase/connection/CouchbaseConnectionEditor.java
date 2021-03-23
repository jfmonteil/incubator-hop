package org.apache.hop.pipeline.transforms.couchbase.connection;


import org.eclipse.swt.SWT;

import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;

import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Combo;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;

import org.eclipse.swt.widgets.Shell;




import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;

import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.PropsUi;

import org.apache.hop.ui.core.widget.PasswordTextVar;

import com.couchbase.client.java.*;
import com.couchbase.client.java.manager.bucket.BucketManager;



public class CouchbaseConnectionEditor extends MetadataEditor<CouchbaseConnection> {
  private static Class<?> PKG = CouchbaseConnection.class; // for i18n purposes, needed by Translator2!!

  private CouchbaseConnection couchbaseConnection;

  private IVariables variables;
  private Shell parent;

  // Connection properties
  //
  private Text wName;
  private TextVar wHostname;
  private TextVar wPort;
  private TextVar wUsername;
  private TextVar wPassword;
  private Combo wBucket;
  private Combo m_wIsCloud;
  private Button getBucketButton;

  
  Control lastControl;

  private PropsUi props;

  private int middle;
  private int margin;

  private boolean ok;

  public CouchbaseConnectionEditor(
  HopGui hopGui, MetadataManager<CouchbaseConnection> manager, CouchbaseConnection couchbaseConnection) {
    super(hopGui, manager, couchbaseConnection);
  }



  @Override
  public void createControl(Composite composite) {
    PropsUi props = PropsUi.getInstance();

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN + 2;

    IVariables variables = getHopGui().getVariables();

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;
	  

    // The name
    Label wlName = new Label( composite, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "CouchbaseConnectionDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );
    lastControl = wName;
	
    Label wlIsCloud = new Label( composite, SWT.RIGHT );
    wlIsCloud.setText(
        BaseMessages.getString(PKG, "CouchbaseConnectionDialog.IsCloud.Label" ) );
    props.setLook( wlIsCloud);
    FormData fdClean = new FormData();
    fdClean.top = new FormAttachment( lastControl, margin );
    fdClean.left = new FormAttachment( 0, 0 );
    fdClean.right = new FormAttachment( middle, -margin );
    wlIsCloud.setLayoutData( fdClean );
    m_wIsCloud = new Combo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wIsCloud );
	fdClean = new FormData();
    fdClean.top = new FormAttachment( lastControl, margin );
    fdClean.left = new FormAttachment( middle, 0 );
    fdClean.right = new FormAttachment( 100, 0 );
    m_wIsCloud.setLayoutData( fdClean );
    lastControl = m_wIsCloud;

    // The Hostname
    Label wlHostname = new Label( composite, SWT.RIGHT );
    props.setLook( wlHostname );
    wlHostname.setText( BaseMessages.getString( PKG, "CouchbaseConnectionDialog.Hostname.Label" ) );
    FormData fdlHostname = new FormData();
    fdlHostname.top = new FormAttachment( lastControl, margin );
    fdlHostname.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlHostname.right = new FormAttachment( middle, -margin );
    wlHostname.setLayoutData( fdlHostname );
    wHostname = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    FormData fdHostname = new FormData();
    fdHostname.top = new FormAttachment( wlHostname, 0, SWT.CENTER );
    fdHostname.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdHostname.right = new FormAttachment( 95, 0 );
    wHostname.setLayoutData( fdHostname );
    lastControl = wHostname;

    // port?
    Label wlPort = new Label( composite, SWT.RIGHT );
    props.setLook( wlPort );
    wlPort.setText( BaseMessages.getString( PKG, "CouchbaseConnectionDialog.Port.Label" ) );
    FormData fdlPort = new FormData();
    fdlPort.top = new FormAttachment( lastControl, margin );
    fdlPort.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlPort.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData( fdlPort );
    wPort = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    FormData fdPort = new FormData();
    fdPort.top = new FormAttachment( wlPort, 0, SWT.CENTER );
    fdPort.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdPort.right = new FormAttachment( 95, 0 );
    wPort.setLayoutData( fdPort );
    lastControl = wPort;

    // Username
    Label wlUsername = new Label( composite, SWT.RIGHT );
    wlUsername.setText( BaseMessages.getString( PKG, "CouchbaseConnectionDialog.UserName.Label" ) );
    props.setLook( wlUsername );
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment( lastControl, margin );
    fdlUsername.left = new FormAttachment( 0, 0 );
    fdlUsername.right = new FormAttachment( middle, -margin );
    wlUsername.setLayoutData( fdlUsername );
    wUsername = new TextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUsername );
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment( wlUsername, 0, SWT.CENTER );
    fdUsername.left = new FormAttachment( middle, 0 );
    fdUsername.right = new FormAttachment( 95, 0 );
    wUsername.setLayoutData( fdUsername );
    lastControl = wUsername;

    // Password
    Label wlPassword = new Label( composite, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "CouchbaseConnectionDialog.Password.Label" ) );
    props.setLook( wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment( wUsername, margin );
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fdlPassword );
    wPassword = new PasswordTextVar( variables, composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment( wlPassword, 0, SWT.CENTER );
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.right = new FormAttachment( 95, 0 );
    wPassword.setLayoutData( fdPassword );
    lastControl = wPassword;

     // Bucket
    Label wlBucket = new Label( composite, SWT.RIGHT );
    wlBucket.setText( BaseMessages.getString( PKG, "CouchbaseConnectionDialog.Bucket.Label" ) );
    props.setLook( wlBucket );
    FormData fdlBucket = new FormData();
    fdlBucket.top = new FormAttachment( wPassword, margin );
    fdlBucket.left = new FormAttachment( 0, 0 );
    fdlBucket.right = new FormAttachment( middle, -margin );
    wlBucket.setLayoutData( fdlBucket );
	
	getBucketButton = new Button(composite, SWT.PUSH | SWT.CENTER);
	getBucketButton.setText("Get Buckets");
	props.setLook(getBucketButton);
	FormData getBucketButtonData = new FormData();
	getBucketButtonData.top = new FormAttachment(lastControl, margin);
	getBucketButtonData.right = new FormAttachment(100, 0);
	getBucketButton.setLayoutData(getBucketButtonData);

	
    wBucket = new Combo(composite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBucket );
    FormData fdBucket = new FormData();
    fdBucket.top = new FormAttachment( wlBucket, 0, SWT.CENTER );
    fdBucket.left = new FormAttachment( middle, 0 );
    fdBucket.right = new FormAttachment( getBucketButton, -margin  );
    wBucket.setLayoutData( fdBucket );
    lastControl = wBucket;

    setWidgetsContent();

    Control[] controls = {
            wName,
            wHostname,
            wPort,
            m_wIsCloud,
            wUsername,
            wPassword,
            wBucket
    };
    for (Control control : controls) {
      control.addListener(SWT.Modify, e -> setChanged());
    }
    getBucketButton.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        listBuckets();
      }
    } );
            //.addListener(SWT.PUSH,e->listBuckets());
  }


  public void setWidgetsContent() {
    wName.setText( Const.NVL( metadata.getName(), "" ) );
    wHostname.setText( Const.NVL( metadata.getHostname(), "" ) );
    wPort.setText( Const.NVL( metadata.getPort(), "18091" ) );
    wUsername.setText( Const.NVL( metadata.getUsername(), "" ) );
    wPassword.setText( Const.NVL( metadata.getPassword(), "" ) );
    wBucket.setText( Const.NVL( metadata.getBucketName(), "Default" ) );
    List<String> elementNames = new ArrayList<String>();
    elementNames.add("On Premise");
    elementNames.add("Cloud");
    m_wIsCloud.setItems( elementNames.toArray( new String[ 0 ] ) );
    m_wIsCloud.setText( Const.NVL( metadata.getIsCloud(), "On Premise" ) );

    wName.setFocus();
  }


  // Get dialog info in securityService
  public void getWidgetsContent(CouchbaseConnection couchbase) {
    couchbase.setName( wName.getText() );
    couchbase.setHostname( wHostname.getText() );
    couchbase.setPort( wPort.getText() );
    couchbase.setUsername( wUsername.getText() );
    couchbase.setPassword( wPassword.getText() );
	couchbase.setBucketName(wBucket.getText() );
	couchbase.setIsCloud(m_wIsCloud.getText());
  }

  public void test() {
    IVariables variables = manager.getVariables();
    CouchbaseConnection couchbase = new CouchbaseConnection( variables ); // parent as variable space
    try {
      getWidgetsContent(couchbase);
      couchbase.test(variables);
      MessageBox box = new MessageBox( hopGui.getShell(), SWT.OK );
      box.setText( "OK" );
      String message = "Connection successful!" + Const.CR;
      message += Const.CR;
      message += "Hostname : " + couchbase.getRealHostname(variables)+", port : "+couchbase.getRealPort(variables)+", user : "+couchbase.getRealUsername(variables)+" bucket : "+couchbase.getRealBucket(variables);
      box.setMessage( message );
      box.open();
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error connecting to Couchbase with Hostname '" + couchbase.getRealHostname(variables)+"', port "+couchbase.getRealPort(variables)+", and username '"+couchbase.getRealUsername(variables)+" bucket : "+couchbase.getRealBucket(variables), e );
    }
  }
  
  public void listBuckets()
  {
    IVariables variables = manager.getVariables();
    CouchbaseConnection couchbase = new CouchbaseConnection( variables );

    try{
      getWidgetsContent(couchbase);
      Cluster cluster= couchbase.connectToCouchbaseCluster(variables);
	  BucketManager bucketManager = cluster.buckets();
      Set<String> bucketNames = bucketManager.getAllBuckets().keySet();
	  List<String> elementNames = new ArrayList<String>();
      
	  for (String bucketName : bucketNames) {
               elementNames.add(bucketName);
		  }
      wBucket.setItems( elementNames.toArray( new String[ 0 ] ) );
	  } catch ( Exception e ) {
			new ErrorDialog( hopGui.getShell(), "Error", "Error getting Buckets with Hostname '" + couchbase.getRealHostname(variables)+"', port "+couchbase.getRealPort(variables)+", and username '"+couchbase.getRealUsername(variables)+" bucket : "+couchbase.getRealBucket(variables), e );
      }
  }
  @Override
  public Button[] createButtonsForButtonBar(Composite composite) {
    Button wTest = new Button(composite, SWT.PUSH);
    wTest.setText(BaseMessages.getString(PKG, "System.Button.Test"));
    wTest.addListener(SWT.Selection, e -> test());
    return new Button[] {wTest};
  }
  @Override
  public boolean setFocus() {
    if (wName == null || wName.isDisposed()) {
      return false;
    }
    return wName.setFocus();
  }

  public void setChanged() {
    if (this.isChanged == false) {
      this.isChanged = true;
      MetadataPerspective.getInstance().updateEditor(this);
    }
  }
}
