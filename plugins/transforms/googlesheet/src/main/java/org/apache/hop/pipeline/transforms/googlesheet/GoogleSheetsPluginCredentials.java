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

import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.core.Const;



import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.api.client.util.Base64;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.security.KeyStore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Describe your step plugin.
 * 
 */
public class GoogleSheetsPluginCredentials {	
    
	
	public static Credential getCredentialsJson(String scope,String jsonCredentialPath) throws IOException {
            
			Credential credential=null;
	        //InputStream in = GoogleSheetsPluginCredentials.class.getResourceAsStream("/plugins/transforms/googlesheet/credentials/client_secret.json");//pentaho-sheets-261911-18ce0057e3d3.json
            //logBasic("Getting credential json file from :"+Const.getKettleDirectory());
            InputStream in=null;
			try{
		       in = HopVfs.getInputStream(jsonCredentialPath);//Const.getKettleDirectory() + "/client_secret.json");
			}  catch (Exception e) {
			//throw new HopFileException("Exception",e.getMessage(),e);
		    }
			
			if (in == null) {
               throw new FileNotFoundException("Resource not found:"+ jsonCredentialPath);			   
            }
			credential = GoogleCredential.fromStream(in).createScoped(Collections.singleton(scope));
            return credential;
	}
	

}
