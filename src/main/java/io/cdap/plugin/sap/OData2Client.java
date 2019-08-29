/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sap;

import com.google.common.base.Strings;
import io.cdap.plugin.sap.exception.ODataException;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Provides handy methods to consume OData v2 services using Apache Olingo v2 as Client Library.
 */
public class OData2Client {

  private static final String METADATA = "$metadata";
  private static final String SEPARATOR = "/";

  private String rootUrl;
  private String username;
  private String password;

  /**
   * @param rootUrl  URL of the OData service. The URL must end with an external service name
   *                 (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).
   * @param username username for basic authentication.
   * @param password password for basic authentication.
   */
  public OData2Client(String rootUrl, String username, String password) {
    this.rootUrl = rootUrl;
    this.username = username;
    this.password = password;
  }

  /**
   * Get {@link ODataFeed} for the specified entity set name.
   *
   * @param entitySetName entity set name.
   * @return {@link ODataFeed} for the specified entity set name.
   * @throws ODataException if the specified entity set cannot be read.
   */
  public ODataFeed readEntitySet(String entitySetName) {
    Edm metadata = getMetadata();

    try {
      EdmEntitySet entitySet = metadata.getDefaultEntityContainer().getEntitySet(entitySetName);
      String entitySetUrl = rootUrl + SEPARATOR + entitySetName;
      HttpURLConnection connection = connect(entitySetUrl, username, password);
      InputStream content = (InputStream) connection.getContent();

      return EntityProvider.readFeed(MediaType.APPLICATION_XML, entitySet, content,
                                     EntityProviderReadProperties.init().build());
    } catch (IOException | EdmException | EntityProviderException e) {
      throw new ODataException(String.format("Unable to read '%s' entity set.", entitySetName), e);
    }
  }

  // TODO query

  /**
   * Get OData service metadata.
   *
   * @return OData service metadata.
   * @throws ODataException if the metadata cannot be fetched.
   */
  public Edm getMetadata() {
    String metadataUrl = rootUrl + SEPARATOR + METADATA;
    HttpURLConnection connection = connect(metadataUrl, username, password);
    try {
      InputStream content = connection.getInputStream();
      return EntityProvider.readMetadata(content, false);
    } catch (IOException | EntityProviderException e) {
      throw new ODataException("Unable to get metadata: " + e.getMessage(), e);
    }
  }

  /**
   * Get {@link EdmEntityType} for the specified entity set name.
   *
   * @param entitySetName entity set name.
   * @return {@link EdmEntityType} for the specified entity set name.
   * @throws ODataException if the {@link EdmEntityType} does not exists or cannot be fetched.
   */
  public EdmEntityType getEntitySetType(String entitySetName) {
    Edm edm = getMetadata();
    try {
      List<EdmEntitySet> entitySets = edm.getDefaultEntityContainer().getEntitySets();
      for (EdmEntitySet entitySet : entitySets) {
        if (entitySetName.equals(entitySet.getName())) {
          return entitySet.getEntityType();
        }
      }
    } catch (EdmException e) {
      throw new ODataException("Unable to get entity set type: " + e.getMessage(), e);
    }

    throw new ODataException(String.format("Unable to find entity type for '%s' entity set.", entitySetName));
  }

  private HttpURLConnection connect(String url, String username, String password) {
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod(HttpMethod.GET);
      connection.setRequestProperty(HttpHeaders.ACCEPT, MediaType.APPLICATION_XML);
      if (!Strings.isNullOrEmpty(username) || !Strings.isNullOrEmpty(password)) {
        byte[] credentials = (username + ":" + password).getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(credentials);
        connection.setRequestProperty(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
      }
      connection.connect();
      return connection;
    } catch (IOException e) {
      throw new ODataException(String.format("Unable to connect to '%s'.", url), e);
    }
  }
}
