package org.apache.ddlutils.model;

public interface IndexableModelObject extends StructureObject {

  public Column findColumn(String name);

}
