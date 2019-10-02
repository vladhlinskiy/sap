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

package io.cdap.plugin.sap.odata.odata4;

import com.google.common.base.Strings;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * OData annotation metadata.
 */
public class OData4Annotation extends ODataAnnotation {

  private final CsdlAnnotation annotation;

  public OData4Annotation(CsdlAnnotation annotation) {
    this.annotation = annotation;
  }

  public String getTerm() {
    return annotation.getTerm();
  }

  public String getQualifier() {
    return annotation.getQualifier();
  }

  public CsdlExpression getExpression() {
    return annotation.getExpression();
  }

  /**
   * Returns map of nested annotations by name. Empty map will be returned if there is no nested annotations.
   *
   * @return map of nested annotations by name. Empty map will be returned if there is no nested annotations.
   */
  public Map<String, OData4Annotation> getAnnotations() {
    List<CsdlAnnotation> annotations = annotation.getAnnotations();
    if (annotations == null || annotations.isEmpty()) {
      return Collections.emptyMap();
    }

    return annotations.stream()
      .map(OData4Annotation::new)
      .collect(Collectors.toMap(OData4Annotation::getName, Function.identity()));
  }

  /**
   * Replaces any character that are not one of [A-Z][a-z][0-9] or _ with an underscore (_).
   *
   * @return annotation name
   */
  @Override
  public String getName() {
    String qualifier = annotation.getQualifier();
    String termName = annotation.getTerm();
    String annotationName = Strings.isNullOrEmpty(qualifier) ? termName : qualifier + "_" + termName;
    return annotationName.toLowerCase().replaceAll("[^A-Za-z0-9]", "_");
  }
}